"""Download + cleaning transform for us_treasury_fiscaldata (shared by the
recurring pipeline and the one-shot bootstrap in
``models/us_treasury_fiscaldata/code/``).

Pure functions (no Prefect) so they are importable and unit-testable. The
FiscalData API is keyless and paginated; each endpoint publishes a field
dictionary (``meta.dataTypes`` / ``meta.labels``). Schema and column order come
from the architecture CSVs (the single source of truth), never from the raw
records, so the pipeline and the bootstrap cannot drift.

Every table is fetched in full and written as a full replace — the API serves
the complete history cheaply, which avoids the incremental-append traps
([[reference_append_upload_overwrites_same_named_parts]],
[[reference_incremental_window_with_overwrite_erases_history]]). Each table is
polled independently so a daily debt refresh does not reprocess the monthly MTS.
"""

import json
import logging
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

from pipelines.datasets.us_treasury_fiscaldata.constants import constants

log = logging.getLogger("us_treasury_fiscaldata")

API_BASE = constants.API_BASE.value
PAGE_SIZE = constants.PAGE_SIZE.value
ENDPOINTS = constants.ENDPOINTS.value
_ARCH = constants.ARCHITECTURE_DIR.value

# The FiscalData API returns the literal string "null" for missing values.
_NULL = "null"


# ── download ────────────────────────────────────────────────────────────────
def fetch_endpoint(path: str) -> tuple[list[dict], dict]:
    """Fetch every page of one FiscalData endpoint, sorted by record_date.

    Args:
        path: Endpoint path under the API base, e.g.
            ``"v2/accounting/od/debt_to_penny"``.

    Returns:
        A ``(records, labels)`` tuple: all rows across every page, and the
        endpoint's ``meta.labels`` map (field name -> human-readable label).

    Raises:
        requests.HTTPError: If any page request fails.
    """
    url = f"{API_BASE}/{path}"
    records: list[dict] = []
    labels: dict = {}
    page = 1
    while True:
        r = requests.get(
            url,
            params={
                "sort": "record_date",
                "page[number]": page,
                "page[size]": PAGE_SIZE,
            },
            timeout=300,
        )
        r.raise_for_status()
        payload = r.json()
        if page == 1:
            labels = payload.get("meta", {}).get("labels", {})
        chunk = payload.get("data", [])
        records.extend(chunk)
        total_pages = payload.get("meta", {}).get("total-pages", 1)
        if page >= total_pages or not chunk:
            break
        page += 1
    log.info(f"{path}: fetched {len(records):,} records over {page} page(s)")
    return records, labels


def download_table(table: str, input_dir: Path) -> Path:
    """Fetch a table's full history and cache it under ``input_dir``.

    Each table maps to a single FiscalData endpoint (a key of
    ``constants.ENDPOINTS``); the four MTS tables each read one ``mts_table_*``.

    Args:
        table: Table slug.
        input_dir: Directory to cache the raw JSON into; created if absent.

    Returns:
        Path to the cached ``<table>.json`` file.
    """
    input_dir.mkdir(parents=True, exist_ok=True)
    all_records: list[dict] = []
    for path in ENDPOINTS[table]:
        records, _ = fetch_endpoint(path)
        all_records.extend(records)
    out = input_dir / f"{table}.json"
    out.write_text(json.dumps({"records": all_records}))
    return out


def _load_raw(table: str, input_dir: Path) -> list[dict]:
    """Read a table's cached raw JSON back as a list of records."""
    return json.loads((input_dir / f"{table}.json").read_text())["records"]


# ── schema helpers ──────────────────────────────────────────────────────────
def read_arch(table: str) -> list[dict]:
    """Read a table's architecture CSV — the schema source of truth.

    Args:
        table: Table slug, matching the CSV filename.

    Returns:
        One dict per column, in architecture order.
    """
    import csv

    with open(_ARCH / f"{table}.csv", newline="") as fh:
        return list(csv.DictReader(fh))


def _nn(v):
    """Normalize the API's string values: literal ``"null"`` and ``""`` -> None."""
    if v is None or v == _NULL or v == "":
        return None
    return v


def _finalize(df: pd.DataFrame, table: str) -> pd.DataFrame:
    """Reduce to architecture columns, in order, as clean all-STRING values.

    Every column is kept verbatim as the source string (with the literal
    ``"null"`` mapped to a real ``None``); nothing is round-tripped through a
    numeric dtype. That preserves the exact decimal text the API publishes —
    "Debt to the Penny" amounts keep their cents, and no value acquires
    scientific notation or a ``"1959.0"`` tail — which is what all-STRING staging
    wants, since the dbt model ``safe_cast``s every column to its real type.

    Args:
        df: Cleaned frame carrying at least the architecture columns.
        table: Table slug, for the architecture lookup.

    Returns:
        The frame reduced to architecture columns, in order, all object/None.
    """
    order = [a["name"] for a in read_arch(table)]
    for name in order:
        df[name] = df[name].map(_nn).astype("object")
    return df[order]


# ── per-table cleaning transforms ───────────────────────────────────────────
def clean_debt_outstanding(records: list[dict]) -> pd.DataFrame:
    """Build the ``debt_outstanding`` table from debt_to_penny records."""
    rows = [
        {
            "year": r["record_date"][:4],
            "record_date": r["record_date"],
            "total_public_debt_outstanding": _nn(
                r.get("tot_pub_debt_out_amt")
            ),
            "debt_held_by_public": _nn(r.get("debt_held_public_amt")),
            "intragovernmental_holdings": _nn(r.get("intragov_hold_amt")),
        }
        for r in records
    ]
    return _finalize(pd.DataFrame(rows), "debt_outstanding")


def clean_historical_debt_outstanding(records: list[dict]) -> pd.DataFrame:
    """Build ``historical_debt_outstanding`` from the annual debt_outstanding records."""
    rows = [
        {
            "year": r["record_date"][:4],
            "record_date": r["record_date"],
            "fiscal_year": _nn(r.get("record_fiscal_year")),
            "debt_outstanding": _nn(r.get("debt_outstanding_amt")),
        }
        for r in records
    ]
    return _finalize(pd.DataFrame(rows), "historical_debt_outstanding")


def clean_average_interest_rate(records: list[dict]) -> pd.DataFrame:
    """Build the ``average_interest_rate`` table from avg_interest_rates records."""
    rows = [
        {
            "year": r["record_date"][:4],
            "month": str(int(r["record_date"][5:7])),
            "record_date": r["record_date"],
            "fiscal_year": _nn(r.get("record_fiscal_year")),
            "security_type": _nn(r.get("security_type_desc")),
            "security_desc": _nn(r.get("security_desc")),
            "avg_interest_rate": _nn(r.get("avg_interest_rate_amt")),
        }
        for r in records
    ]
    return _finalize(pd.DataFrame(rows), "average_interest_rate")


def clean_exchange_rate(records: list[dict]) -> pd.DataFrame:
    """Build the ``exchange_rate`` table from rates_of_exchange records."""
    rows = [
        {
            "year": r["record_date"][:4],
            "record_date": r["record_date"],
            "effective_date": _nn(r.get("effective_date")),
            "country": _nn(r.get("country")),
            "currency": _nn(r.get("currency")),
            "country_currency_desc": _nn(r.get("country_currency_desc")),
            "exchange_rate": _nn(r.get("exchange_rate")),
        }
        for r in records
    ]
    # The source occasionally emits a byte-for-byte duplicate row (one across the
    # full history); the natural key is (record_date, country, currency,
    # effective_date), since a rate may be revised mid-quarter with a new
    # effective_date. Drop exact duplicates so the key is unique.
    df = pd.DataFrame(rows).drop_duplicates(ignore_index=True)
    return _finalize(df, "exchange_rate")


def _clean_mts(records: list[dict], table: str) -> pd.DataFrame:
    """Build one wide Monthly Treasury Statement table from its endpoint records.

    All four MTS tables share the same key/hierarchy skeleton and differ only in
    their dollar-amount columns; ``constants.MTS_AMOUNTS[table]`` maps each source
    ``*_amt`` field to a readable output column. Rows are kept wide (no melt), so
    each amount keeps its own named, typed column and stays safely aggregatable.

    Args:
        records: Rows from the table's ``mts_table_*`` endpoint.
        table: MTS table slug (a key of ``constants.MTS_AMOUNTS``).

    Returns:
        The wide frame in architecture column order.
    """
    amounts = constants.MTS_AMOUNTS.value[table]
    rows = []
    for r in records:
        row = {
            "year": r["record_date"][:4],
            "month": str(int(r["record_date"][5:7])),
            "record_date": r["record_date"],
            "fiscal_year": _nn(r.get("record_fiscal_year")),
            "src_line_nbr": _nn(r.get("src_line_nbr")),
            "parent_id": _nn(r.get("parent_id")),
            "classification_id": _nn(r.get("classification_id")),
            "classification_desc": _nn(r.get("classification_desc")),
            "sequence_level_nbr": _nn(r.get("sequence_level_nbr")),
        }
        for src, out in amounts.items():
            row[out] = _nn(r.get(src))
        rows.append(row)
    return _finalize(pd.DataFrame(rows), table)


CLEANERS = {
    "debt_outstanding": clean_debt_outstanding,
    "historical_debt_outstanding": clean_historical_debt_outstanding,
    "mts_summary": lambda r: _clean_mts(r, "mts_summary"),
    "mts_receipts": lambda r: _clean_mts(r, "mts_receipts"),
    "mts_outlays": lambda r: _clean_mts(r, "mts_outlays"),
    "mts_means_of_financing": lambda r: _clean_mts(
        r, "mts_means_of_financing"
    ),
    "average_interest_rate": clean_average_interest_rate,
    "exchange_rate": clean_exchange_rate,
}


# ── write ───────────────────────────────────────────────────────────────────
def write_partitioned(df: pd.DataFrame, table: str, output_dir: Path) -> Path:
    """Write a table as all-STRING Snappy Parquet, hive-partitioned by year.

    Staging is all-STRING by Data Basis convention — the dbt model ``safe_cast``s
    every column, and ``gcs.dump_header`` stringifies the header BigQuery infers
    the staging schema from, so typed parquet is rejected
    ([[reference_empty_parquet_partition_poisons_staging_schema]]). Values pass
    through the architecture's real types first (so ``year`` serializes as
    ``"1959"`` not ``"1959.0"``) and are then cast to string via arrow — never
    ``astype(str)``, which renders NaN as the literal ``"nan"``.

    Args:
        df: Cleaned, architecture-ordered frame from a cleaner.
        table: Table slug, for the architecture lookup and output path.
        output_dir: Root output directory.

    Returns:
        The table's directory, ``<output_dir>/<table>/year=<YYYY>/data.parquet``.
    """
    arch = read_arch(table)
    order = [a["name"] for a in arch]
    string_schema = pa.schema([pa.field(a["name"], pa.string()) for a in arch])

    out = df[order]
    tdir = output_dir / table
    for year, g in out.groupby("year", sort=True):
        # pyrefly: ignore [bad-argument-type]
        pdir = tdir / f"year={int(year)}"
        pdir.mkdir(parents=True, exist_ok=True)
        at = pa.Table.from_pandas(
            g, schema=string_schema, preserve_index=False
        )
        pq.write_table(at, pdir / "data.parquet", compression="snappy")
    log.info(f"{table}: {len(out):,} rows -> {tdir}")
    return tdir


# ── orchestration entry points ──────────────────────────────────────────────
def _max_date(df: pd.DataFrame, table: str) -> str | None:
    """Return the latest period in a table, formatted for its source poll.

    Daily/quarterly tables report ``YYYY-MM-DD`` (their record_date is a real
    day); monthly tables report ``YYYY-MM``.
    """
    if not len(df):
        return None
    md = df["record_date"].dropna().max()
    fmt = constants.DATE_FORMAT.value[table]
    if fmt == "%Y":
        return str(md)[:4]
    if fmt == "%Y-%m":
        return str(md)[:7]
    return str(md)[:10]


def clean_table(table: str, input_dir: Path, output_dir: Path) -> dict:
    """Clean one table from its cached raw JSON and write partitioned parquet.

    Args:
        table: Data-table slug.
        input_dir: Root of the cached raw JSON.
        output_dir: Root output directory.

    Returns:
        ``{"path": <dir>, "max_date": <str|None>}`` — the poll uses ``max_date``.
    """
    records = _load_raw(table, input_dir)
    df = CLEANERS[table](records)
    path = write_partitioned(df, table, output_dir)
    return {"path": path, "max_date": _max_date(df, table)}


def clean_all(input_dir: Path, output_dir: Path) -> dict:
    """Build every data table from the cached raw JSON (bootstrap entry point).

    Args:
        input_dir: Root of the cached raw JSON.
        output_dir: Root output directory.

    Returns:
        Mapping of table slug to its ``clean_table`` result.
    """
    return {
        table: clean_table(table, input_dir, output_dir)
        for table in constants.DATA_TABLES.value
    }


def download_all(input_dir: Path) -> Path:
    """Fetch every data table's full history into ``input_dir`` (bootstrap)."""
    for table in constants.DATA_TABLES.value:
        download_table(table, input_dir)
    return input_dir
