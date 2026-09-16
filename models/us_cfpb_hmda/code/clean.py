"""Clean one year of a HMDA LAR table into year-partitioned parquet (duckdb, out-of-core).

  uv run --with duckdb python clean.py loan_application_register 2024
  uv run --with duckdb python clean.py loan_application_register_legacy 2017

Reads input/<era>_<year>.csv, projects/renames/reorders/casts every column per the
architecture TSV (source of truth), and writes
  output/<table>/year=<YYYY>/data.parquet   (year excluded from the file -> hive partition).

Casting rules: STRING -> trimmed varchar, blank -> NULL, raw codes kept verbatim.
INT64/FLOAT64 -> TRY_CAST (source sentinels "NA"/"Exempt"/blank -> NULL). Amount columns
reported in thousands (see common.MULTIPLY_1000) are multiplied by 1000 so unit=USD holds.
Columns absent from a given year's header become typed NULLs (schema is stable across years).
"""

import csv
import sys
from pathlib import Path

import duckdb
from common import (
    INPUT,
    LEGACY,
    LEGACY_YEARS,
    MODERN,
    MODERN_YEARS,
    MULTIPLY_1000,
    OUTPUT,
    load_cols,
)

BQ_TO_DUCK = {"INT64": "BIGINT", "FLOAT64": "DOUBLE", "STRING": "VARCHAR"}


def _header(csv_path: Path) -> set[str]:
    with open(csv_path, encoding="utf-8-sig", newline="") as fh:
        return set(next(csv.reader(fh)))


def _expr(col, present: set[str], table: str) -> str:
    """Build the SELECT expression for one architecture column."""
    q = '"' + col.original.replace('"', '""') + '"'
    duck = BQ_TO_DUCK[col.bq_type]
    if col.original not in present:
        return f"CAST(NULL AS {duck}) AS {col.name}"
    if col.bq_type == "STRING":
        return f"nullif(trim({q}), '') AS {col.name}"
    base = f"TRY_CAST(nullif(trim({q}), '') AS {duck})"
    if (table, col.name) in MULTIPLY_1000:
        base = f"({base}) * 1000"
        if col.bq_type == "INT64":
            base = f"CAST({base} AS BIGINT)"
    return f"{base} AS {col.name}"


def clean(table: str, year: int) -> Path:
    era = "modern" if table == MODERN else "legacy"
    src = INPUT / f"{era}_{year}.csv"
    if not src.exists():
        raise SystemExit(f"missing input {src} — run download.py first")

    cols = [
        c for c in load_cols(table) if c.name != "year"
    ]  # year -> hive partition
    present = _header(src)
    exprs = ",\n    ".join(_expr(c, present, table) for c in cols)

    missing = [c.name for c in cols if c.original not in present]
    if missing:
        print(
            f"  NOTE {table} {year}: {len(missing)} col(s) absent -> NULL: {missing}"
        )

    out_dir = OUTPUT / table / f"year={year}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / "data.parquet"

    tmp = OUTPUT.parent / "duck_tmp"
    tmp.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    # Memory discipline: stream the COPY instead of buffering the whole scan to preserve
    # row order (we don't need order), cap RAM, and allow generous on-disk spill. Without
    # preserve_insertion_order=false a 26M x 99 projection balloons to tens of GB.
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET threads=2")
    con.execute("SET memory_limit='4GB'")
    con.execute(f"SET temp_directory='{tmp}'")
    con.execute("SET max_temp_directory_size='80GB'")
    read = (
        f"read_csv('{src}', header=true, all_varchar=true, sample_size=-1, "
        f"quote='\"', escape='\"', null_padding=true, ignore_errors=false)"
    )
    sql = (
        f"COPY (SELECT\n    {exprs}\n FROM {read}) "
        f"TO '{out}' (FORMAT PARQUET, COMPRESSION SNAPPY, ROW_GROUP_SIZE 100000)"
    )
    con.execute(sql)
    n = con.execute(f"SELECT count(*) FROM read_parquet('{out}')").fetchone()[
        0
    ]
    ncols = (
        con.execute(f"SELECT * FROM read_parquet('{out}') LIMIT 0")
        .fetchdf()
        .shape[1]
    )
    con.close()
    print(f"  wrote {out}  rows={n:,}  cols={ncols}")
    return out


if __name__ == "__main__":
    if len(sys.argv) != 3:
        raise SystemExit(__doc__)
    table, year = sys.argv[1], int(sys.argv[2])
    valid = (
        MODERN_YEARS
        if table == MODERN
        else LEGACY_YEARS
        if table == LEGACY
        else None
    )
    if valid is None:
        raise SystemExit(f"unknown table {table!r}")
    if year not in valid:
        raise SystemExit(f"{table} year must be in {valid[0]}..{valid[-1]}")
    clean(table, year)
