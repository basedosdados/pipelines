"""Download + cleaning transform for us_fhfa_hpi (shared by the pipeline and the
one-shot bootstrap in models/us_fhfa_hpi/code/).

Pure functions (no Prefect) so they are importable and unit-testable. The
recurring pipeline wraps them in @task (see tasks.py); the bootstrap CLI imports
`clean_all` directly. Schema/column order come from the architecture CSVs (the
single source of truth).
"""

import csv
import logging
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

from pipelines.datasets.us_fhfa_hpi.constants import constants

log = logging.getLogger("us_fhfa_hpi")

PA = {"STRING": pa.string(), "INT64": pa.int64(), "FLOAT64": pa.float64()}

_ARCH = constants.ARCHITECTURE_DIR.value
_UA = {"User-Agent": constants.USER_AGENT.value}
_ANNUAL_FILES = constants.ANNUAL_FILES.value
_MASTER_TABLES = constants.MASTER_TABLES.value
_HEADER_ROW = constants.ANNUAL_HEADER_ROW.value

# The census tract file ships as CSV with its own short column names; map them
# onto the workbook headers so one architecture-driven rename covers every
# annual table.
_TRACT_HEADERS = {
    "year": "Year",
    "annual_change": "Annual Change (%)",
    "hpi": "HPI",
    "hpi1990": "HPI with 1990 base",
    "hpi2000": "HPI with 2000 base",
}

# Dictionary entries for the two coded columns of the master-derived tables.
_DICTIONARY = {
    "index_type": {
        "traditional": "Traditional index, the series FHFA reports in its press releases",
        "developmental": "Developmental index, published as experimental",
        "distress-free": "Distress-free index, excluding sales of distressed properties",
        "non-metro": "Index for the nonmetropolitan areas of the state",
        "manufactured": "Index for manufactured homes",
    },
    "index_flavor": {
        "purchase-only": "Purchase-only index, estimated from sale prices alone",
        "all-transactions": "All-transactions index, estimated from sale prices and appraisals",
        "expanded-data": (
            "Expanded-data index, adding FHA and county recorder transactions below the "
            "annual loan limit ceiling"
        ),
    },
}


# ── architecture ────────────────────────────────────────────────────────────
def read_arch(table: str) -> list[dict]:
    """Read a table's architecture CSV — the schema source of truth.

    Column order and BigQuery types come from here, never from the raw files, so
    the pipeline and the one-shot bootstrap cannot drift apart.

    Args:
        table: Table slug (e.g. ``"quarterly_state"``), matching the CSV filename.

    Returns:
        One dict per column, in architecture order.
    """
    with open(_ARCH / f"{table}.csv", newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def _rename_map(table: str) -> dict[str, str]:
    """Map the source's own column names onto the architecture's, for one table."""
    return {
        a["original_name"]: a["name"]
        for a in read_arch(table)
        if a["original_name"]
    }


# ── download ────────────────────────────────────────────────────────────────
def _fetch(url: str, dest: Path) -> Path:
    dest.parent.mkdir(parents=True, exist_ok=True)
    with requests.get(url, headers=_UA, stream=True, timeout=600) as r:
        r.raise_for_status()
        with dest.open("wb") as fh:
            for chunk in r.iter_content(chunk_size=1 << 20):
                fh.write(chunk)
    log.info(f"downloaded {url} -> {dest} ({dest.stat().st_size:,} bytes)")
    return dest


def download_master(input_dir: Path) -> Path:
    """Fetch ``hpi_master.csv``, the monthly + quarterly master file."""
    return _fetch(constants.MASTER_URL.value, input_dir / "hpi_master.csv")


def download_annual(input_dir: Path) -> dict[str, Path]:
    """Fetch every annual developmental index file, keyed by table slug."""
    base = constants.ANNUAL_BASE_URL.value
    out = {}
    for table, (fname, _) in _ANNUAL_FILES.items():
        out[table] = _fetch(f"{base}/{fname}", input_dir / f"annual_{fname}")
    return out


def download_all(input_dir: Path) -> Path:
    """Fetch the master file and every annual file into ``input_dir``."""
    input_dir.mkdir(parents=True, exist_ok=True)
    download_master(input_dir)
    download_annual(input_dir)
    return input_dir


# ── clean ───────────────────────────────────────────────────────────────────
def _coerce(df: pd.DataFrame, table: str) -> pd.DataFrame:
    """Apply the architecture's types and column order to a cleaned frame."""
    arch = read_arch(table)
    for a in arch:
        name, kind = a["name"], a["bigquery_type"]
        if name not in df:
            raise KeyError(
                f"{table}: architecture column {name!r} missing after cleaning"
            )
        if kind == "INT64":
            df[name] = pd.to_numeric(df[name], errors="coerce").astype("Int64")
        elif kind == "FLOAT64":
            df[name] = pd.to_numeric(df[name], errors="coerce").astype(
                "float64"
            )
        else:
            # Strip and normalise to None. `pd.isna` is checked explicitly: a
            # NaN reaching `str()` would be written as the literal "nan", which
            # `safe_cast` cannot turn back into NULL.
            df[name] = (
                df[name]
                .astype("object")
                .map(
                    lambda v: None if pd.isna(v) else (str(v).strip() or None)
                )
            )
    return df[[a["name"] for a in arch]]


def build_master(input_dir: Path) -> dict[str, pd.DataFrame]:
    """Split ``hpi_master.csv`` into its four geography-level tables.

    The source stacks every published series in one file. Splitting by
    ``frequency`` and ``level`` gives each table a single observation level and
    drops the columns that are empty at that level.

    Args:
        input_dir: Directory holding ``hpi_master.csv``.

    Returns:
        Table slug -> cleaned frame, in architecture column order.
    """
    raw = pd.read_csv(input_dir / "hpi_master.csv", dtype=str)
    # Every row outside the metro tables carries a literal tab as its note.
    raw["note"] = raw["note"].str.strip().replace("", pd.NA)

    out = {}
    for table, flt in _MASTER_TABLES.items():
        sub = raw[raw["frequency"] == flt["frequency"]]
        if "level" in flt:
            sub = sub[sub["level"].isin(flt["level"])]
        sub = sub.rename(columns=_rename_map(table)).copy()
        out[table] = _coerce(sub, table)
        log.info(f"{table}: {len(out[table]):,} rows")
    return out


def _read_annual(input_dir: Path, table: str) -> pd.DataFrame:
    fname, sheet = _ANNUAL_FILES[table]
    path = input_dir / f"annual_{fname}"
    if sheet is None:
        df = pd.read_csv(path, dtype=str).rename(columns=_TRACT_HEADERS)
    else:
        df = pd.read_excel(
            path, sheet_name=sheet, header=_HEADER_ROW, dtype=str
        )
    df = df.dropna(how="all")
    # Workbooks occasionally trail a footnote row; a valid row always has a year.
    return df[df["Year"].astype(str).str.fullmatch(r"\d{4}")]


def build_annual(input_dir: Path) -> dict[str, pd.DataFrame]:
    """Clean every annual developmental index file into its table."""
    out = {}
    for table in _ANNUAL_FILES:
        df = _read_annual(input_dir, table).rename(columns=_rename_map(table))
        out[table] = _coerce(df, table)
        log.info(f"{table}: {len(out[table]):,} rows")
    return out


def build_dicionario(master: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Build the code -> label table for the two coded master columns.

    Only the codes that actually occur in each table are emitted, so the
    dictionary covers the data exactly rather than the union of every level.

    Args:
        master: Table slug -> cleaned frame, from :func:`build_master`.
    """
    rows = []
    for table, df in master.items():
        for column, labels in _DICTIONARY.items():
            for key in sorted(df[column].dropna().unique()):
                rows.append(
                    {
                        "id_tabela": table,
                        "nome_coluna": column,
                        "chave": key,
                        "cobertura_temporal": "",
                        "valor": labels[key],
                    }
                )
    return _coerce(pd.DataFrame(rows), "dicionario")


# ── write ───────────────────────────────────────────────────────────────────
def write_partitioned(df: pd.DataFrame, table: str, output_dir: Path) -> Path:
    """Write a table as all-STRING Snappy Parquet, hive-partitioned by year.

    Staging is all-STRING by Data Basis convention — the dbt model ``safe_cast``s
    every column to its real type, and ``pipelines.utils.gcs.dump_header``
    stringifies the header file that BigQuery infers the staging schema from.
    Emitting typed parquet against that STRING schema makes BigQuery reject the
    files ("Parquet column ... does not match the target cpp_type STRING_PIECE").

    Values pass through the architecture's real types first, so ``year``
    serializes as ``"1975"`` rather than ``"1975.0"``, and only then cast to
    string via arrow — never ``astype(str)``, which would render a NULL as the
    literal ``"nan"`` and defeat the dbt ``safe_cast``.

    ``dicionario`` has no year column and is written unpartitioned.

    Args:
        df: Cleaned frame in architecture column order.
        table: Table slug, used for the architecture lookup and output path.
        output_dir: Root output directory.

    Returns:
        The table's directory, ``<output_dir>/<table>/year=<YYYY>/data.parquet``.
    """
    arch = read_arch(table)
    typed = pa.schema(
        [pa.field(a["name"], PA[a["bigquery_type"]]) for a in arch]
    )
    strings = pa.schema([pa.field(a["name"], pa.string()) for a in arch])
    tdir = output_dir / table
    tdir.mkdir(parents=True, exist_ok=True)

    def _write(frame: pd.DataFrame, dest: Path) -> None:
        at = pa.Table.from_pandas(frame, schema=typed, preserve_index=False)
        pq.write_table(at.cast(strings), dest, compression="snappy")

    if "year" not in df.columns:
        _write(df, tdir / "data.parquet")
    else:
        for year, g in df.groupby("year", sort=True):
            pdir = tdir / f"year={int(year)}"
            pdir.mkdir(parents=True, exist_ok=True)
            _write(g, pdir / "data.parquet")
    log.info(f"{table}: {len(df):,} rows -> {tdir}")
    return tdir


def clean_master(input_dir: Path, output_dir: Path) -> dict:
    """Clean the master file into its four tables plus the dictionary.

    Args:
        input_dir: Directory holding ``hpi_master.csv``.
        output_dir: Root output directory for the parquet tables.

    Returns:
        ``{"paths": {table: str}, "counts": {table: int}, "max_year_month": "YYYY-MM"}``.
        Paths are strings so Prefect can serialize the task result.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    master = build_master(input_dir)
    frames = {"dicionario": build_dicionario(master), **master}
    paths, counts = {}, {}
    for table in ["dicionario", *_MASTER_TABLES]:
        paths[table] = str(write_partitioned(frames[table], table, output_dir))
        counts[table] = len(frames[table])
    monthly = frames["monthly_national"]
    year = int(monthly["year"].max())
    month = int(monthly.loc[monthly["year"] == year, "month"].max())
    return {
        "paths": paths,
        "counts": counts,
        "max_year_month": f"{year:04d}-{month:02d}",
    }


def clean_annual(input_dir: Path, output_dir: Path) -> dict:
    """Clean the annual developmental index files into their seven tables.

    Args:
        input_dir: Directory holding the downloaded annual files.
        output_dir: Root output directory for the parquet tables.

    Returns:
        ``{"paths": {table: str}, "counts": {table: int}, "max_year": "YYYY"}``.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    frames = build_annual(input_dir)
    paths, counts = {}, {}
    for table in _ANNUAL_FILES:
        paths[table] = str(write_partitioned(frames[table], table, output_dir))
        counts[table] = len(frames[table])
    max_year = max(int(df["year"].max()) for df in frames.values())
    return {"paths": paths, "counts": counts, "max_year": f"{max_year:04d}"}


def clean_all(input_dir: Path, output_dir: Path) -> dict:
    """Clean every FHFA HPI file and write the partitioned parquet output.

    Args:
        input_dir: Directory holding the downloaded source files.
        output_dir: Root output directory for the parquet tables.

    Returns:
        The merged result of :func:`clean_master` and :func:`clean_annual`.
    """
    m = clean_master(input_dir, output_dir)
    a = clean_annual(input_dir, output_dir)
    return {
        "paths": {**m["paths"], **a["paths"]},
        "counts": {**m["counts"], **a["counts"]},
        "max_year_month": m["max_year_month"],
        "max_year": a["max_year"],
    }


# ── source freshness ────────────────────────────────────────────────────────
def master_max_period(input_dir: Path) -> str:
    """Latest month covered by the master file, as ``YYYY-MM``.

    The master file is republished with every monthly HPI release, so its latest
    monthly period is what the source-poll compares against.
    """
    raw = pd.read_csv(
        input_dir / "hpi_master.csv",
        dtype=str,
        usecols=["frequency", "yr", "period"],
    )
    m = raw[raw["frequency"] == "monthly"]
    year = m["yr"].astype(int).max()
    month = m.loc[m["yr"].astype(int) == year, "period"].astype(int).max()
    return f"{year:04d}-{month:02d}"
