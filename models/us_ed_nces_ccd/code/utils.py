#!/usr/bin/env python3
"""Pure download and cleaning functions for us_ed_nces_ccd.

No Prefect imports. The one-shot onboarding entrypoint (`clean_data.py`) and the
recurring Prefect pipeline (`pipelines/datasets/us_ed_nces_ccd/`) both import
from here, so the transform exists once.

The transform is expressed as DuckDB SQL over the source CSVs and streams
straight to Parquet, so a 900 MB enrollment file never lands in a dataframe.

Staging output is all-STRING by house convention: the dbt model `safe_cast`s
each column to its architecture type, and `gcs.py::dump_header` stringifies the
staging header anyway, so typed Parquet would be rejected. Casting runs through
Arrow rather than `astype(str)` so NULL stays NULL instead of becoming the
literal "nan", and integer-valued columns serialise as "1959" rather than
"1959.0".
"""

from __future__ import annotations

import csv
import json
import os
import shutil
import time
import urllib.request
from collections.abc import Iterator
from pathlib import Path

import duckdb

# pyrefly: ignore [missing-import]
import schema

BASE_URL = "https://educationdata.urban.org/csv/ccd"

#: Source file per table. The enrollment extract is published one file per year.
BULK_FILES = {
    "school": "schools_ccd_directory.csv",
    "school_district": "school-districts_lea_directory.csv",
    "district_finance": "districts_ccd_finance.csv",
}
ENROLLMENT_FILE = "schools_ccd_enrollment_{year}.csv"

ENROLLMENT_YEARS = range(1986, 2025)


def data_dir() -> Path:
    return Path(
        os.environ.get(
            "CCD_DATA_DIR", Path.home() / "Downloads" / "us_ed_nces_ccd_data"
        )
    )


def input_dir() -> Path:
    d = data_dir() / "input"
    d.mkdir(parents=True, exist_ok=True)
    return d


def output_dir() -> Path:
    d = data_dir() / "output"
    d.mkdir(parents=True, exist_ok=True)
    return d


# ---------------------------------------------------------------------------
# download
# ---------------------------------------------------------------------------


def download(
    file_name: str, dest: Path | None = None, attempts: int = 5
) -> Path:
    """Fetch one Urban bulk CSV, skipping the download if it is already present.

    The portal stalls a large transfer often enough to matter -- the connection
    does not error, it simply stops delivering bytes -- so the socket timeout is
    kept short and applied per read rather than to the request as a whole, and a
    stalled attempt is retried from scratch.
    """
    dest = dest or (input_dir() / file_name)
    if dest.exists() and dest.stat().st_size > 0:
        return dest
    tmp = dest.with_suffix(dest.suffix + ".part")
    url = f"{BASE_URL}/{file_name}"
    last: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            req = urllib.request.Request(
                url, headers={"User-Agent": "Mozilla/5.0"}
            )
            with (
                urllib.request.urlopen(req, timeout=120) as r,
                tmp.open("wb") as fh,
            ):
                shutil.copyfileobj(r, fh, length=8 << 20)
            tmp.rename(dest)
            return dest
        except Exception as exc:
            last = exc
            tmp.unlink(missing_ok=True)
            if attempt < attempts:
                time.sleep(5 * attempt)
    raise RuntimeError(
        f"download failed after {attempts} attempts: {url}"
    ) from last


# ---------------------------------------------------------------------------
# SQL fragments
# ---------------------------------------------------------------------------


def _read(path: Path) -> str:
    """Read the CSV entirely as VARCHAR; every cast is made explicit below."""
    return f"read_csv('{path.as_posix()}', all_varchar=true, header=true)"


def _nullif_sentinels(expr: str) -> str:
    """Map Urban's -1 / -2 / -3 to NULL.

    Compares numerically because the same sentinel is written "-1" in an integer
    column and "-1.0" in a float column. Applied per column, never to `grade`,
    where -1 is the prekindergarten category.
    """
    codes = ", ".join(str(c) for c in schema.SENTINELS)
    return f"nullif_sentinel({expr}, [{codes}])"


def _pad(expr: str, width: int) -> str:
    return f"lpad(trim({expr}), {width}, '0')"


def _school_id_sql(col: str = "ncessch") -> str:
    """Zero-pad the NCES school id and repair the one malformed source value."""
    fixups = " ".join(
        f"when trim({col}) = '{bad}' then '{good}'"
        for bad, good in schema.NCESSCH_FIXUPS.items()
    )
    return f"case {fixups} else {_pad(col, 12)} end"


def _select_expr(col: schema.Col) -> str:
    """The staging expression for one column: sentinel-cleaned, padded, STRING."""
    src = col.source
    if src is None:
        raise ValueError(f"{col.name} is derived and has no source expression")

    if col.name == "school_id":
        return f"{_school_id_sql(src)} as {col.name}"
    if col.name in ("agency_id", "state_id", "county_id"):
        width = schema.PAD[col.name]
        # A sentinel in an identifier column must become NULL, not a padded
        # "0000-1": the finance extract writes -1/-2 into `leaid` for records
        # with no usable agency id.
        cleaned = _nullif_sentinels(f"try_cast({src} as double)")
        return (
            f"case when {cleaned} is null then null else {_pad(src, width)} end "
            f"as {col.name}"
        )
    if col.name == "year":
        return f"cast(try_cast({src} as double) as int) as year"
    if col.name in schema.SENTINEL_EXEMPT:
        return f"cast(cast(try_cast({src} as double) as bigint) as varchar) as {col.name}"

    cleaned = _nullif_sentinels(f"try_cast({src} as double)")
    blank = f"{src} is null or trim({src}) = ''"

    if col.type == "STRING":
        if col.dictionary:
            # A dictionary code arrives as "3" in one year and "3.0" in the
            # next, so it is normalised to a bare integer string and matches a
            # single dictionary key. Safe here precisely because a code carries
            # no significant leading zero.
            return (
                f"case when {blank} then null "
                f"when try_cast({src} as double) is null then trim({src}) "
                f"else cast(cast({cleaned} as bigint) as varchar) end as {col.name}"
            )
        # Every other STRING column is an identifier, a code with significant
        # leading zeros (ZIP, CBSA, the 14-digit Census government id) or free
        # text. Kept verbatim apart from trimming: normalising these through a
        # numeric cast silently drops the leading zero, turning ZIP 01005 into
        # 1005 and Census id 01504840100000 into 1504840100000.
        return (
            f"case when {blank} then null "
            f"when try_cast({src} as double) is not null "
            f"and {cleaned} is null then null "
            f"else trim({src}) end as {col.name}"
        )
    if col.type == "INT64":
        return f"cast(cast({cleaned} as bigint) as varchar) as {col.name}"
    return f"cast({cleaned} as varchar) as {col.name}"


def register_helpers(con: duckdb.DuckDBPyConnection) -> None:
    """Register the sentinel macro. A macro keeps the generated SQL readable."""
    con.execute(
        "create or replace macro nullif_sentinel(v, codes) as "
        "case when v is null then null when list_contains(codes, v) then null else v end"
    )


def connect(
    memory_limit: str = "6GB", threads: int | None = None
) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("set preserve_insertion_order = false")
    con.execute(f"set memory_limit = '{memory_limit}'")
    if threads:
        con.execute(f"set threads = {threads}")
    register_helpers(con)
    return con


# ---------------------------------------------------------------------------
# per-table transforms
# ---------------------------------------------------------------------------


def _row_count(con: duckdb.DuckDBPyConnection, query: str) -> int:
    """Row count of a query. `fetchone` is typed Optional; a count never is."""
    row = con.execute(f"select count(*) from ({query})").fetchone()
    if row is None:  # pragma: no cover - a count always returns one row
        raise RuntimeError("count query returned no row")
    return int(row[0])


def _copy_to_parquet(
    con: duckdb.DuckDBPyConnection, query: str, dest: Path
) -> None:
    dest.mkdir(parents=True, exist_ok=True)
    con.execute(
        f"copy ({query}) to '{(dest / 'data.parquet').as_posix()}' "
        "(format parquet, compression snappy)"
    )


def _body_columns(table: schema.Table) -> list[schema.Col]:
    """Every column except the partition key.

    `year` is carried by the Hive directory name, not by the Parquet body. Left
    in both places BigQuery would see the column twice when it builds the
    external table over the partitioned prefix.
    """
    return [c for c in table.columns if c.name != table.partition]


def clean_wide_table(
    con: duckdb.DuckDBPyConnection,
    table: schema.Table,
    source: Path,
    out_root: Path,
    year: int,
) -> int:
    """Clean one year of a directly-mapped table.

    Written one Hive partition per year so a re-run only rebuilds the years it
    touched, and so the recurring pipeline can append a single year.
    """
    cols = ",\n    ".join(_select_expr(c) for c in _body_columns(table))
    query = (
        f"select\n    {cols}\nfrom {_read(source)}\n"
        f"where cast(try_cast(year as double) as int) = {year}"
    )
    _copy_to_parquet(con, query, out_root / table.slug / f"year={year}")
    return _row_count(con, query)


def clean_enrollment(
    con: duckdb.DuckDBPyConnection, source: Path, out_root: Path, year: int
) -> int:
    """Clean one year of the long school membership extract."""
    cols = ",\n    ".join(
        _select_expr(c) for c in _body_columns(schema.TABLE_ENROLLMENT)
    )
    query = f"select\n    {cols}\nfrom {_read(source)}"
    _copy_to_parquet(
        con, query, out_root / "school_enrollment" / f"year={year}"
    )
    return _row_count(con, query)


def clean_staff(
    con: duckdb.DuckDBPyConnection, source: Path, out_root: Path, year: int
) -> int:
    """Reshape the 27 wide staff FTE columns of the agency directory into long form.

    Rows whose FTE is missing after sentinel cleaning are dropped: a NULL count
    carries no information the absence of the row does not already carry, and
    keeping them would multiply the table roughly fivefold.
    """
    unpivot = ",\n        ".join(
        f"('{code}', {_nullif_sentinels(f'try_cast({src} as double)')})"
        for code, src, *_ in schema.STAFF_CATEGORIES
    )
    query = f"""
select
    {_select_expr(schema.AGENCY_ID)},
    {_select_expr(schema.STATE_ID)},
    u.category as staff_category,
    cast(u.fte as varchar) as staff_fte
from {_read(source)} as t,
     unnest([
        {unpivot}
     ]::struct(category varchar, fte double)[]) as _u(u)
where cast(try_cast(t.year as double) as int) = {year}
  and u.fte is not null
"""
    _copy_to_parquet(con, query, out_root / "staff" / f"year={year}")
    return _row_count(con, query)


def write_dictionary(
    con: duckdb.DuckDBPyConnection, values_csv: Path, out_root: Path
) -> int:
    query = f"""
select
    cast(id_tabela as varchar) id_tabela,
    cast(nome_coluna as varchar) nome_coluna,
    cast(chave as varchar) chave,
    cast(nullif(cobertura_temporal, '') as varchar) cobertura_temporal,
    cast(valor as varchar) valor
from {_read(values_csv)}
"""
    _copy_to_parquet(con, query, out_root / "dicionario")
    return _row_count(con, query)


# ---------------------------------------------------------------------------
# API fallback for a corrupt bulk extract
# ---------------------------------------------------------------------------

#: Years whose bulk enrollment CSV is unusable and must come from the API.
#:
#: The 1987 file writes 726,746 of its 883,551 `ncessch` values in Excel
#: scientific notation ("1.00008E+11"), rounded to six significant digits. That
#: collapses thousands of distinct schools onto one identifier -- 1,665 schools
#: onto 04.30003E+11 alone -- and the school number is not recoverable from the
#: row, since only the LEAID survives. The portal's own API returns the correct
#: 12-character ids for the same year and the same 883,551 rows, so the defect
#: is in the CSV export, not the data.
API_FALLBACK_YEARS = frozenset({1987})

API_BASE = "https://educationdata.urban.org/api/v1/schools/ccd/enrollment"

#: Grade path segments the API accepts, in the order the CSV lists them.
#: Prekindergarten is `grade-pk`, not `grade--1`, which returns HTTP 500.
API_GRADES = [
    "grade-pk",
    *[f"grade-{n}" for n in range(13)],
    "grade-15",
    "grade-99",
]

#: Columns of the bulk CSV, reproduced so the API output is a drop-in
#: replacement for the transform that reads it.
ENROLLMENT_CSV_COLUMNS = [
    "year",
    "ncessch",
    "ncessch_num",
    "leaid",
    "fips",
    "grade",
    "race",
    "sex",
    "enrollment",
]


def _api_pages(url: str, attempts: int = 5) -> Iterator[list[dict]]:
    """Yield each page of an Urban API result set, following `next`."""
    next_url: str | None = url
    while next_url:
        payload: dict = {}
        for attempt in range(1, attempts + 1):
            try:
                req = urllib.request.Request(
                    next_url, headers={"User-Agent": "Mozilla/5.0"}
                )
                with urllib.request.urlopen(req, timeout=180) as fh:
                    payload = json.load(fh)
                break
            except Exception:
                if attempt == attempts:
                    raise
                time.sleep(5 * attempt)
        yield payload["results"]
        next_url = payload.get("next")


def download_enrollment_via_api(year: int, dest: Path | None = None) -> Path:
    """Rebuild one year of the enrollment extract from the API, as CSV.

    Writes the same columns in the same order as the bulk file, so
    :func:`clean_enrollment` reads it without knowing the difference. The
    `/race/` sub-endpoint is used for every grade: where a year has no race
    detail it simply returns the race=99 rows, so one code path covers both.
    """
    dest = dest or (input_dir() / ENROLLMENT_FILE.format(year=year))
    if dest.exists() and dest.stat().st_size > 0:
        return dest

    tmp = dest.with_suffix(dest.suffix + ".part")
    written = 0
    with tmp.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(
            fh, fieldnames=ENROLLMENT_CSV_COLUMNS, extrasaction="ignore"
        )
        writer.writeheader()
        for grade in API_GRADES:
            url = f"{API_BASE}/{year}/{grade}/race/?limit=10000"
            for rows in _api_pages(url):
                writer.writerows(rows)
                written += len(rows)
    tmp.rename(dest)
    print(f"  {year}: {written:,} rows rebuilt from the API")
    return dest


def fetch_enrollment(year: int) -> Path:
    """The enrollment extract for one year, from whichever source is sound."""
    if year in API_FALLBACK_YEARS:
        return download_enrollment_via_api(year)
    return download(ENROLLMENT_FILE.format(year=year))
