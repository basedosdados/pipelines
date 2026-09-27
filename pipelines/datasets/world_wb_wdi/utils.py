"""Download + cleaning transform for world_wb_wdi (shared by the pipeline and the
one-shot bootstrap in models/world_wb_wdi/code/).

Pure functions (no Prefect) so they are importable and unit-testable. The
recurring pipeline wraps them in @task (see tasks.py); the bootstrap imports
``clean_all`` directly. Column order and BigQuery types come from the architecture
CSVs (the single source of truth), never from the raw headers.

Staging output is **all-STRING** by Data Basis convention: ``gcs.dump_header``
stringifies the one-row header BigQuery infers the staging schema from, so typed
parquet is rejected. The dbt model ``safe_cast``s every column to its real type.
Raw source value strings are preserved verbatim (never round-tripped through
float), so no precision is lost and a NULL never becomes the literal ``"nan"``.
"""

import csv
import logging
import shutil
import zipfile
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

from pipelines.datasets.world_wb_wdi.constants import constants

log = logging.getLogger("world_wb_wdi")

_ARCH = constants.ARCHITECTURE_DIR.value
_SRC = constants.SOURCE_FILES.value
_PARTITIONED = constants.PARTITIONED_TABLES.value


# ── download ────────────────────────────────────────────────────────────────
def download_source(input_dir: Path) -> Path:
    """Download and extract WDI_CSV.zip into ``input_dir``.

    Skips the download when the zip is already present (bootstrap re-runs). The
    World Bank republishes the whole archive on each release, so this always
    fetches the current full history.

    Args:
        input_dir: Directory to download and extract into; created if absent.

    Returns:
        The same ``input_dir``, for chaining.

    Raises:
        requests.HTTPError: If the archive fails to download.
    """
    input_dir.mkdir(parents=True, exist_ok=True)
    zip_path = input_dir / constants.ZIP_NAME.value
    if not zip_path.exists():
        log.info(f"downloading {constants.SOURCE_URL.value}")
        r = requests.get(constants.SOURCE_URL.value, timeout=600)
        r.raise_for_status()
        zip_path.write_bytes(r.content)
    root = input_dir.resolve()
    with zipfile.ZipFile(zip_path) as zf:
        # Guard against Zip Slip: refuse any member that resolves outside root.
        for member in zf.namelist():
            if not (root / member).resolve().is_relative_to(root):
                raise ValueError(f"unsafe path in archive: {member!r}")
        zf.extractall(input_dir)
    log.info(f"extracted WDI CSVs -> {input_dir}")
    return input_dir


# ── schema ──────────────────────────────────────────────────────────────────
def read_arch(table: str) -> list[dict]:
    """Read a table's architecture CSV — the schema source of truth.

    Args:
        table: Table slug (e.g. ``"data"``), matching the CSV filename.

    Returns:
        One dict per column, in architecture order.
    """
    with open(_ARCH / f"{table}.csv", newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def _read_csv(input_dir: Path, table: str) -> pd.DataFrame:
    """Read a source CSV as strings, stripping the BOM, only ``""`` is NULL.

    ``keep_default_na=False`` with ``na_values=[""]`` keeps literal tokens such as
    ``"NA"``/``"NULL"`` as strings — they are real content in WDI text fields —
    and turns only genuinely empty cells into NULL.
    """
    return pd.read_csv(
        input_dir / _SRC[table],
        dtype=str,
        encoding="utf-8-sig",
        keep_default_na=False,
        na_values=[""],
    )


def _strip_yr(s: pd.Series) -> pd.Series:
    """``"YR2010"`` -> integer ``2010`` (WDI year encoding in footnote/time).

    Case-insensitive: the footnote file mixes ``YR2002`` and ``yr2002``.
    """
    return s.str.replace(r"(?i)yr", "", regex=True).astype(int)


# ── builders (one per output table) ─────────────────────────────────────────
def build_data(input_dir: Path) -> pd.DataFrame:
    """Melt WDICSV.csv wide->long into (year, country_id, indicator_id, value).

    The wide file carries one row per (country, indicator) with one column per
    year 1960..latest. Empty cells (indicator not reported for that country-year)
    are dropped rather than stored as NULL rows. Values are kept as their raw
    source strings — no float round-trip, so precision is preserved.
    """
    df = _read_csv(input_dir, "data")
    year_cols = [c for c in df.columns if c.strip().isdigit()]
    long = df.melt(
        id_vars=["Country Code", "Indicator Code"],
        value_vars=year_cols,
        var_name="year",
        value_name="value",
    )
    long = long.dropna(subset=["value"])
    return pd.DataFrame(
        {
            "year": long["year"].astype(int),
            "country_id": long["Country Code"],
            "indicator_id": long["Indicator Code"],
            "value": long["value"],
        }
    )


def build_indicators(input_dir: Path) -> pd.DataFrame:
    """Reshape WDISeries.csv into the indicators metadata table (drop name)."""
    df = _read_csv(input_dir, "indicators")
    rename = {a["original_name"]: a["name"] for a in read_arch("indicators")}
    df = df.rename(columns={c: rename.get(c, c) for c in df.columns})
    order = [a["name"] for a in read_arch("indicators")]
    return df[order]


def build_country_indicator(input_dir: Path) -> pd.DataFrame:
    """Reshape WDIcountry-series.csv into country_indicator."""
    df = _read_csv(input_dir, "country_indicator")
    return pd.DataFrame(
        {
            "country_id": df["CountryCode"],
            "indicator_id": df["SeriesCode"],
            "description": df["DESCRIPTION"],
        }
    )


def build_footnote(input_dir: Path) -> pd.DataFrame:
    """Reshape WDIfootnote.csv into footnote; ``Year`` 'YR####' -> int."""
    df = _read_csv(input_dir, "footnote")
    df = df[df["Year"].notna()]
    return pd.DataFrame(
        {
            "year": _strip_yr(df["Year"]),
            "country_id": df["CountryCode"],
            "indicator_id": df["SeriesCode"],
            "description": df["DESCRIPTION"],
        }
    )


def build_indicator_time(input_dir: Path) -> pd.DataFrame:
    """Reshape WDIseries-time.csv into indicator_time; ``Year`` 'YR####' -> int."""
    df = _read_csv(input_dir, "indicator_time")
    df = df[df["Year"].notna()]
    return pd.DataFrame(
        {
            "year": _strip_yr(df["Year"]),
            "indicator_id": df["SeriesCode"],
            "description": df["DESCRIPTION"],
        }
    )


def build_dicionario(input_dir: Path) -> pd.DataFrame:
    """Derive the dictionary: ``data.indicator_id`` code -> Indicator Name.

    WDI's only per-dataset coded column is ``indicator_id``; the dictionary maps
    each code to its human-readable name. Column names stay Portuguese
    (``id_tabela``, ``nome_coluna``, ``chave``, ``cobertura_temporal``, ``valor``)
    even though this dataset is English — the platform's dictionary renderer
    expects that schema.
    """
    df = _read_csv(input_dir, "indicators")
    df = df[["Series Code", "Indicator Name"]].dropna(subset=["Series Code"])
    return pd.DataFrame(
        {
            "id_tabela": "data",
            "nome_coluna": "indicator_id",
            "chave": df["Series Code"].to_numpy(),
            "cobertura_temporal": None,
            "valor": df["Indicator Name"].to_numpy(),
        }
    )


BUILDERS = {
    "data": build_data,
    "indicators": build_indicators,
    "country_indicator": build_country_indicator,
    "footnote": build_footnote,
    "indicator_time": build_indicator_time,
    "dicionario": build_dicionario,
}


# ── write (all-STRING parquet) ──────────────────────────────────────────────
def _to_string_table(df: pd.DataFrame, order: list[str]) -> pa.Table:
    """Cast a frame to an all-STRING pyarrow table in architecture order.

    NULLs stay NULL (never ``"nan"``); ``year`` serializes as ``"1960"`` (never
    ``"1960.0"``) because it is a clean Python int before ``str``.
    """
    arrays = []
    for name in order:
        col = df[name]
        vals = [None if pd.isna(v) else str(v) for v in col]
        arrays.append(pa.array(vals, type=pa.string()))
    return pa.table(
        arrays, schema=pa.schema([pa.field(n, pa.string()) for n in order])
    )


def write_table(df: pd.DataFrame, table: str, output_dir: Path) -> Path:
    """Write one table as Snappy all-STRING parquet.

    ``data`` and ``footnote`` are hive-partitioned by year
    (``<table>/year=YYYY/data.parquet``); the rest are a single
    ``<table>/data.parquet``. The year column is kept inside the file too, matching
    the repo's proven partitioned-upload pattern (us_bls_cpi).

    Args:
        df: The built table, columns matching the architecture names.
        table: Table slug.
        output_dir: Root output directory.

    Returns:
        The table's output directory.
    """
    order = [a["name"] for a in read_arch(table)]
    df = df[order].reset_index(drop=True)
    tdir = output_dir / table
    # Full-history snapshot: clear any prior output so a year dropped by the
    # latest archive cannot leave a stale partition behind (bootstrap re-runs;
    # the pipeline uses a fresh tempdir each run).
    shutil.rmtree(tdir, ignore_errors=True)
    if table in _PARTITIONED:
        for year, g in df.groupby("year", sort=True):
            # pyrefly: ignore [bad-argument-type]
            pdir = tdir / f"year={int(year)}"
            pdir.mkdir(parents=True, exist_ok=True)
            pq.write_table(
                _to_string_table(g, order),
                pdir / "data.parquet",
                compression="snappy",
            )
    else:
        tdir.mkdir(parents=True, exist_ok=True)
        pq.write_table(
            _to_string_table(df, order),
            tdir / "data.parquet",
            compression="snappy",
        )
    log.info(f"{table}: {len(df):,} rows -> {tdir}")
    return tdir


def clean_all(input_dir: Path, output_dir: Path) -> dict:
    """Build all six tables from the extracted WDI CSVs.

    The single entry point shared by the recurring pipeline (via
    :func:`pipelines.datasets.world_wb_wdi.tasks.clean_wdi`) and the one-shot
    bootstrap in ``models/world_wb_wdi/code/``.

    Args:
        input_dir: Directory holding the extracted WDI CSVs.
        output_dir: Root output directory.

    Returns:
        Mapping of table slug to output directory, plus ``"max_year"`` — the
        latest year present in ``data`` (drives the source-update poll), and
        ``"counts"`` — rows per table.
    """
    result: dict[str, object] = {}
    counts: dict[str, int] = {}
    max_year = None
    for table in constants.ALL_TABLES.value:
        df = BUILDERS[table](input_dir)
        counts[table] = len(df)
        result[table] = write_table(df, table, output_dir)
        if table == "data" and len(df):
            max_year = int(df["year"].max())
    result["max_year"] = max_year
    result["counts"] = counts
    log.info(f"row counts: {counts}; max_year={max_year}")
    return result
