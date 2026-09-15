"""Clean the ANGA OData inventory JSON into LONG partitioned parquet.

One output table per classification family, each LONG:
    year, geography, <ordered category-level columns>, [gas], emissions_gg

Design decisions (see the onboarding plan / dataset memory):
  - `year` (INT64) is the OData InventoryYear_ID, the financial-year-ending
    (1990 = FY1989-90 ... 2024 = FY2023-24). Partition column.
  - `geography` (STRING): 'australia' (national aggregate) or one of the eight
    states/territories, plus 'et' (external/other territories). Kept as a plain
    STRING because the national aggregate cannot take an id_state; to be linked
    to br_bd_diretorios_au.state where applicable.
  - The constant classification root (Level_0, e.g. "Total UNFCCC") carries no
    information and is dropped. Deeper level columns are kept only where the
    source populates them in at least one row of any jurisdiction.
  - `emissions_gg` (FLOAT64) is the source measure in gigagrams (Gg). The gas
    dimension says Gg-of-what; the Gg CO2-equivalent aggregate appears as
    gas = 'CO2-e - AR5' (Mt CO2-e = emissions_gg / 1000 on those rows). We do
    NOT convert, which would corrupt the single-gas rows.
  - Values are kept faithfully; NULL stays NULL (arrow, not astype(str)) so the
    staging all-STRING cast and the dbt safe_cast round-trip cleanly.

Pure functions (no Prefect) so the recurring pipeline can import them.

Usage:
    uv run python models/au_dcceew_greenhouse/code/clean.py
"""

import json
import os
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Jurisdiction codes, mirroring download.JURISDICTIONS.values() (kept local so
# this module has no cross-script import; the recurring pipeline centralizes both).
JUR_CODES = [
    "australia", "act", "et", "nsw", "nt", "qld", "sa", "tas", "vic", "wa",
]

OUTPUT_ROOT = Path(
    os.environ.get(
        "AU_DCCEEW_GREENHOUSE_DATA",
        Path.home() / "Downloads" / "au_dcceew_greenhouse_data",
    )
)

# Which category-hierarchy prefix each table uses, and whether it carries the
# gas hierarchy. The gas hierarchy is 4 levels: Gas_Level_0 is a physical gas
# (CO2, CH4, ...) OR the aggregate 'CO2-e - AR5'; for the CO2-e rows Gas_Level_1..3
# break the CO2-equivalent down by contributing gas (CO2/CH4/N2O/Other ->
# HFC/PFC/SF6 -> species). All four levels are needed to keep the grain unique.
TABLE_SCHEME = {
    "inventory_unfccc": ("UNFCCC_Level_", 11, True),
    "inventory_anzsic": ("ANZSIC_Level_", 7, True),
    "inventory_scope2": ("ScopeTwo_Level_", 3, False),
}
GAS_MAX_LEVELS = 4


def _load_table(input_dir: Path, table_slug: str) -> pd.DataFrame:
    frames = []
    for jur_code in JUR_CODES:
        path = input_dir / table_slug / f"{jur_code}.json"
        rows = json.loads(path.read_text())["value"]
        df = pd.DataFrame(rows)
        df["geography"] = jur_code
        frames.append(df)
    return pd.concat(frames, ignore_index=True)


def _level_columns(
    df: pd.DataFrame, prefix: str, max_levels: int
) -> list[str]:
    """Populated level columns, level 1..N (drop constant level_0 root)."""
    kept = []
    for i in range(1, max_levels):
        col = f"{prefix}{i}"
        if col in df.columns and df[col].notna().any():
            kept.append(col)
    return kept


def clean_table(input_dir: Path, table_slug: str) -> pd.DataFrame:
    prefix, max_levels, has_gas = TABLE_SCHEME[table_slug]
    df = _load_table(input_dir, table_slug)
    level_cols = _level_columns(df, prefix, max_levels)

    # rename source columns -> snake_case output columns
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

    # sort for stable output
    out = out.sort_values(
        ["year", "geography", *cat_cols, *gas_cols], kind="stable"
    ).reset_index(drop=True)
    return out


def _to_string_table(df: pd.DataFrame) -> pa.Table:
    """All-STRING arrow table, stable column order, NULL preserved (house rule).

    Pass real types through first so year serializes as '1990' not '1990.0',
    then cast via arrow (never astype(str), which writes the literal 'nan').
    """
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
) -> None:
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


def clean_all(
    input_dir: Path | None = None, output_dir: Path | None = None
) -> dict[str, int]:
    input_dir = input_dir or (OUTPUT_ROOT / "input")
    output_dir = output_dir or (OUTPUT_ROOT / "output")
    counts = {}
    for table_slug in TABLE_SCHEME:
        df = clean_table(input_dir, table_slug)
        write_partitioned(df, table_slug, output_dir)
        counts[table_slug] = len(df)
        cols = [c for c in df.columns if c != "year"]
        print(
            f"{table_slug}: {len(df):,} rows | years {int(df['year'].min())}-{int(df['year'].max())} "
            f"| geos {df['geography'].nunique()} | cols year,{','.join(cols)}",
            flush=True,
        )
    return counts


if __name__ == "__main__":
    clean_all()
