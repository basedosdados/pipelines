"""Download + cleaning transform for world_bis_property_prices (shared by the
pipeline and the one-shot bootstrap in models/world_bis_property_prices/code/).

Pure functions (no Prefect) so they are importable and unit-testable. The
recurring pipeline wraps them in @task (see tasks.py); the bootstrap CLI imports
`clean_all` directly. Schema/column order come from the architecture CSV (the
single source of truth).
"""

import csv
import io
import logging
import zipfile
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

from pipelines.datasets.world_bis_property_prices.constants import constants

log = logging.getLogger("world_bis_property_prices")

PA = {"STRING": pa.string(), "INT64": pa.int64(), "FLOAT64": pa.float64()}
_ARCH = constants.ARCHITECTURE_DIR.value
_VALUE_LABEL = constants.VALUE_LABEL.value
_MEASURE_BY_UNIT = constants.MEASURE_BY_UNIT.value
_AGGREGATES = constants.AGGREGATES.value
_COUNTRY_ISO3 = constants.COUNTRY_ISO3.value


# ── download ────────────────────────────────────────────────────────────────
def download_flatfile(input_dir: Path) -> Path:
    """Fetch and extract the BIS selected property prices flat CSV.

    The BIS Data Portal serves the whole dataset as one zipped flat CSV. The
    extracted file lands at ``input_dir/<CSV_NAME>``.

    Args:
        input_dir: Directory to download into; created if absent.

    Returns:
        Path to the extracted CSV.

    Raises:
        requests.HTTPError: If the download fails.
    """
    input_dir.mkdir(parents=True, exist_ok=True)
    headers = {"User-Agent": constants.USER_AGENT.value}
    r = requests.get(constants.BULK_URL.value, headers=headers, timeout=300)
    r.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
        name = constants.CSV_NAME.value
        zf.extract(name, input_dir)
    out = input_dir / constants.CSV_NAME.value
    log.info(f"downloaded {out} ({out.stat().st_size:,} bytes)")
    return out


# ── schema ──────────────────────────────────────────────────────────────────
def read_arch(table: str) -> list[dict]:
    """Read a table's architecture CSV — the schema source of truth.

    Column order and BigQuery types come from here, never from the raw file, so
    the pipeline and the one-shot bootstrap cannot drift apart.

    Args:
        table: Table slug (``"price_index"``), matching the CSV filename.

    Returns:
        One dict per column, in architecture order.
    """
    with open(_ARCH / f"{table}.csv", newline="") as fh:
        return list(csv.DictReader(fh))


# ── transform ───────────────────────────────────────────────────────────────
def _code(cell: str) -> str:
    """Return the code half of a BIS ``"code: label"`` cell (before the colon)."""
    return cell.split(": ", 1)[0].strip() if ": " in cell else cell.strip()


def _label(cell: str) -> str:
    """Return the label half of a BIS ``"code: label"`` cell (after the colon)."""
    return cell.split(": ", 1)[1].strip() if ": " in cell else cell.strip()


def build_price_index(input_dir: Path) -> pd.DataFrame:
    """Parse the BIS selected flat CSV into the long ``price_index`` frame.

    The flat CSV carries one row per observation, with each dimension stored as
    a ``"code: label"`` string (e.g. ``"AU: Australia"``). Series identity is
    ``FREQ.REF_AREA.VALUE.UNIT_MEASURE``; the frequency is quarterly throughout,
    so ``TIME_PERIOD`` is always ``"YYYY-Qn"``.

    Args:
        input_dir: Directory holding the extracted CSV.

    Returns:
        One row per (reference area, quarter, value type, measure), with the
        columns declared in the architecture CSV.
    """
    df = pd.read_csv(
        input_dir / constants.CSV_NAME.value, dtype=str, na_filter=False
    )
    # Resolve the "CODE:label" header names to the coded dimension we need.
    col = {c.split(":", 1)[0]: c for c in df.columns}
    freq = df[col["FREQ"]].map(_code)
    if not (freq == "Q").all():
        bad = sorted(set(freq[freq != "Q"]))
        raise ValueError(f"unexpected non-quarterly frequency codes: {bad}")

    area_code = df[col["REF_AREA"]].map(_code)
    area_name = df[col["REF_AREA"]].map(_label)
    value_code = df[col["VALUE"]].map(_code)
    unit_code = df[col["UNIT_MEASURE"]].map(_code)
    unit_label = df[col["UNIT_MEASURE"]].map(_label)
    period = df[col["TIME_PERIOD"]].str.strip()

    year = period.str.slice(0, 4)
    quarter = period.str.slice(6)  # "2008-Q4" -> "4"

    out = pd.DataFrame(
        {
            "year": pd.to_numeric(year, errors="coerce").astype("Int64"),
            "quarter": pd.to_numeric(quarter, errors="coerce").astype("Int64"),
            "country_id": area_code.map(_COUNTRY_ISO3),  # NaN for aggregates
            "reference_area_code": area_code,
            "reference_area_name": area_name,
            "value_type": value_code.map(_VALUE_LABEL),
            "measure": unit_code.map(_MEASURE_BY_UNIT),
            "unit": unit_label,
            "bis_series_key": (
                freq + "." + area_code + "." + value_code + "." + unit_code
            ),
            # to_numeric coerces blank cells to NaN (this source never blanks
            # an observation, but stay defensive).
            "value": pd.to_numeric(df[col["OBS_VALUE"]], errors="coerce"),
        }
    )

    # Keep only individual economies: drop the four BIS aggregates (World,
    # advanced economies, emerging market economies, euro area), which have no
    # ISO3 country. Every remaining row must then map to an ISO3 — fail loud
    # rather than ship a silent NULL country_id.
    out = out[~out["reference_area_code"].isin(_AGGREGATES)].copy()
    unmapped = sorted(
        set(out.loc[out["country_id"].isna(), "reference_area_code"])
    )
    if unmapped:
        raise ValueError(
            f"reference-area codes without an ISO3 mapping: {unmapped}"
        )
    if out["value_type"].isna().any():
        raise ValueError(
            f"unmapped VALUE codes: {sorted(set(value_code[out['value_type'].isna()]))}"
        )
    if out["measure"].isna().any():
        raise ValueError(
            f"unmapped UNIT_MEASURE codes: {sorted(set(unit_code[out['measure'].isna()]))}"
        )

    out = out.sort_values(
        ["reference_area_code", "value_type", "measure", "year", "quarter"],
        ignore_index=True,
    )
    log.info(
        f"price_index: {len(out):,} rows, {out['reference_area_code'].nunique()} areas"
    )
    return out


# ── write ───────────────────────────────────────────────────────────────────
def write_partitioned(df: pd.DataFrame, table: str, output_dir: Path) -> Path:
    """Write a table as all-STRING Snappy Parquet, hive-partitioned by year.

    Staging is all-STRING by Data Basis convention — the dbt model ``safe_cast``s
    every column to its real type, and ``pipelines.utils.gcs.dump_header``
    stringifies the header file BigQuery infers the staging schema from. Values
    pass through the architecture's real types first (so ``year`` serializes as
    ``"1970"`` not ``"1970.0"``), then cast to string via arrow — never
    ``astype(str)``, which would render a NULL as the literal ``"nan"`` and
    defeat the dbt ``safe_cast``. ``country_id`` and ``value`` carry genuine
    NULLs (aggregates have no ISO3; no observation is ever blank in this source).

    Args:
        df: Rows for the table, from :func:`build_price_index`.
        table: Table slug, used for the architecture lookup and output path.
        output_dir: Root output directory.

    Returns:
        The table's directory, ``<output_dir>/<table>/year=<YYYY>/data.parquet``.
    """
    arch = read_arch(table)
    order = [a["name"] for a in arch]
    typed_schema = pa.schema(
        [pa.field(a["name"], PA[a["bigquery_type"]]) for a in arch]
    )
    string_schema = pa.schema([pa.field(a["name"], pa.string()) for a in arch])
    out = df[order]
    tdir = output_dir / table
    for year, g in out.groupby("year", sort=True):
        pdir = tdir / f"year={int(year)}"
        pdir.mkdir(parents=True, exist_ok=True)
        at = pa.Table.from_pandas(g, schema=typed_schema, preserve_index=False)
        at = at.cast(string_schema)
        pq.write_table(at, pdir / "data.parquet", compression="snappy")
    log.info(f"{table}: {len(out):,} rows -> {tdir}")
    return tdir


def clean_all(input_dir: Path, output_dir: Path) -> dict:
    """Build the ``price_index`` table from the downloaded flat file.

    The single entry point shared by the recurring pipeline (via
    :func:`pipelines.datasets.world_bis_property_prices.tasks.clean_bis`) and the
    one-shot bootstrap in ``models/world_bis_property_prices/code/``.

    Args:
        input_dir: Directory holding the extracted CSV.
        output_dir: Root output directory.

    Returns:
        Mapping with ``"price_index"`` -> its output directory,
        ``"max_year_quarter"`` -> the latest ``"YYYY-Qn"`` (human form), and
        ``"max_year_month"`` -> the same period as ``"YYYY-MM"`` with
        month = quarter*3. The source poll compares a ``"%Y-%m"`` string against
        the coverage, which for a YearQuarter column is stored as
        ``MAX(DATE(year, quarter*3, 1))`` — so the pollable form is the
        year-MONTH, not the year-quarter. Both are None if the data is empty.
    """
    df = build_price_index(input_dir)
    result: dict[str, object] = {
        "price_index": write_partitioned(df, "price_index", output_dir)
    }
    if len(df):
        last = df.sort_values(["year", "quarter"]).iloc[-1]
        y, q = int(last["year"]), int(last["quarter"])
        result["max_year_quarter"] = f"{y}-Q{q}"
        result["max_year_month"] = f"{y}-{q * 3:02d}"
    else:
        result["max_year_quarter"] = None
        result["max_year_month"] = None
    return result
