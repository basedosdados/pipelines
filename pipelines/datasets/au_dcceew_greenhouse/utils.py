"""Pure download + cleaning functions for au_dcceew_greenhouse (no Prefect).

Canonical home of the transform. The one-shot onboarding scripts under
models/au_dcceew_greenhouse/code/ import from here, so the logic lives once.

Source: ANGA OData API. Three classification families x ten jurisdictions
(30 entity sets). The server ignores ``$top`` and rejects ``$count``, so each
call streams the whole entity set; we download each in full and filter locally.

Output: one LONG partitioned-parquet table per family —
    year x geography x <category levels> x <gas levels> x emissions_gg
Staging is all-STRING by house convention (the dbt model ``safe_cast``s each
column); real types are passed through arrow first, then cast to string, so
``year`` serializes as ``"1990"`` and NULL stays NULL (never ``astype(str)``,
which would write the literal ``"nan"``).
"""

import json
import time
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

from pipelines.datasets.au_dcceew_greenhouse.constants import constants

BASE = constants.BASE_URL.value
UA = constants.USER_AGENT.value
FAMILIES = constants.FAMILIES.value
JURISDICTIONS = constants.JURISDICTIONS.value
TABLE_SCHEME = constants.TABLE_SCHEME.value
GAS_MAX_LEVELS = constants.GAS_MAX_LEVELS.value


# ---- download -----------------------------------------------------------------
def fetch_entity_set(
    family_prefix: str, jur_token: str, retries: int = 4
) -> bytes:
    """Return the raw JSON bytes of one OData entity set (the whole set)."""
    url = f"{BASE}/{family_prefix}_{jur_token}?$format=json"
    last = None
    for attempt in range(retries):
        try:
            r = requests.get(url, headers={"User-Agent": UA}, timeout=300)
            r.raise_for_status()
            return r.content
        except Exception as e:
            last = e
            time.sleep(3 * (attempt + 1))
    raise RuntimeError(f"failed to fetch {url}: {last}")


def download_all(input_dir: Path) -> Path:
    """Download all 30 entity sets to <input>/<table_slug>/<geography>.json."""
    for family_prefix, table_slug in FAMILIES.items():
        out = input_dir / table_slug
        out.mkdir(parents=True, exist_ok=True)
        for jur_token, jur_code in JURISDICTIONS.items():
            content = fetch_entity_set(family_prefix, jur_token)
            (out / f"{jur_code}.json").write_bytes(content)
            print(
                f"  {table_slug}/{jur_code}: {len(content) / 1e6:.2f} MB",
                flush=True,
            )
    return input_dir


# ---- clean --------------------------------------------------------------------
def _load_table(input_dir: Path, table_slug: str) -> pd.DataFrame:
    frames = []
    for jur_code in JURISDICTIONS.values():
        rows = json.loads(
            (input_dir / table_slug / f"{jur_code}.json").read_text()
        )["value"]
        df = pd.DataFrame(rows)
        df["geography"] = jur_code
        frames.append(df)
    return pd.concat(frames, ignore_index=True)


def _level_columns(
    df: pd.DataFrame, prefix: str, max_levels: int
) -> list[str]:
    """Populated level columns, level 1..N (drop the constant level_0 root)."""
    return [
        f"{prefix}{i}"
        for i in range(1, max_levels)
        if f"{prefix}{i}" in df.columns and df[f"{prefix}{i}"].notna().any()
    ]


def clean_table(input_dir: Path, table_slug: str) -> pd.DataFrame:
    prefix, max_levels, has_gas = TABLE_SCHEME[table_slug]
    df = _load_table(input_dir, table_slug)
    level_cols = _level_columns(df, prefix, max_levels)

    out = pd.DataFrame()
    out["year"] = df["InventoryYear_ID"].astype("Int64")
    out["geography"] = df["geography"].astype("string")
    cat_cols = []
    for idx, col in enumerate(level_cols, start=1):
        newname = f"{prefix.split('_')[0].lower()}_level_{idx}"
        out[newname] = df[col].astype("string")
        cat_cols.append(newname)
    gas_cols = []
    if has_gas:
        for i in range(GAS_MAX_LEVELS):
            newname = f"gas_level_{i}"
            out[newname] = df[f"Gas_Level_{i}"].astype("string")
            gas_cols.append(newname)
    out["emissions_gg"] = pd.to_numeric(df["Gg"], errors="coerce").astype(
        "float64"
    )

    return out.sort_values(
        ["year", "geography", *cat_cols, *gas_cols], kind="stable"
    ).reset_index(drop=True)


def _to_string_table(df: pd.DataFrame) -> pa.Table:
    """All-STRING arrow table, stable column order, NULL preserved (house rule)."""
    typed = {}
    for col in df.columns:
        if col == "year":
            typed[col] = pa.array(df[col].astype("Int64"), type=pa.int64())
        elif col == "emissions_gg":
            typed[col] = pa.array(df[col], type=pa.float64())
        else:
            typed[col] = pa.array(df[col].astype("string"), type=pa.string())
    tbl = pa.table(typed)
    return tbl.cast(pa.schema([(c, pa.string()) for c in df.columns]))


def write_partitioned(
    df: pd.DataFrame, table_slug: str, output_dir: Path
) -> Path:
    """Write hive-partitioned parquet: output/<table>/year=<Y>/data.parquet (all-STRING)."""
    base = output_dir / table_slug
    for y in sorted(df["year"].dropna().astype(int).unique()):
        part = base / f"year={int(y)}"
        part.mkdir(parents=True, exist_ok=True)
        chunk = df[df["year"] == y]
        pq.write_table(
            _to_string_table(chunk.drop(columns=["year"])),
            part / "data.parquet",
            compression="snappy",
        )
    return base


def clean_all(input_dir: Path, output_dir: Path) -> dict:
    """Clean every table to partitioned parquet.

    Returns a mapping of table slug to its output directory, plus ``"max_year"``
    — the latest ``InventoryYear`` present, which drives the source-update poll.
    """
    result: dict = {}
    max_year = 0
    for table_slug in TABLE_SCHEME:
        df = clean_table(input_dir, table_slug)
        result[table_slug] = write_partitioned(df, table_slug, output_dir)
        max_year = max(max_year, int(df["year"].max()))
        print(
            f"{table_slug}: {len(df):,} rows | years {int(df['year'].min())}-{int(df['year'].max())}",
            flush=True,
        )
    result["max_year"] = max_year
    return result
