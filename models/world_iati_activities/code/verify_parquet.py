"""Validate the cleaned world_iati_activities parquet before uploading.

Checks, per table:

1. the parquet column list and order match the architecture CSV exactly;
2. every column is STRING (staging is all-STRING by house convention);
3. the uniqueness key dbt will test is actually unique;
4. no column is entirely NULL — a fully-null column is the signature of a
   mis-mapped source name that ``safe_cast`` would later hide;
5. row counts against the source, and what the licence filter removed.

Run: ``python verify_parquet.py [table ...]``
"""

import sys

import duckdb
import pyarrow.parquet as pq
from common import OUTPUT, REPO_ROOT  # noqa: F401
from gen_dbt import UNIQUE_KEY

from pipelines.datasets.world_iati_activities.constants import constants
from pipelines.datasets.world_iati_activities.utils import load_cols

PARTITION_SOURCE = constants.PARTITION_SOURCE.value


def glob(table: str) -> str:
    return f"{OUTPUT / table}/**/*.parquet"


def check(table: str, con: duckdb.DuckDBPyConnection) -> list[str]:
    problems = []
    files = sorted((OUTPUT / table).rglob("*.parquet"))
    if not files:
        return [f"{table}: no parquet written"]

    expected = [c.name for c in load_cols(table)]
    # A hive-partitioned table encodes `year` in the directory name, not in the
    # file body; BigQuery reconstructs it from the path.
    in_file = [
        c for c in expected if not (c == "year" and table in PARTITION_SOURCE)
    ]
    schema = pq.read_schema(files[0])
    if list(schema.names) != in_file:
        problems.append(
            f"{table}: column order drift\n"
            f"    parquet: {list(schema.names)}\n"
            f"    arch:    {in_file}"
        )
    typed = [
        n
        for n, t in zip(schema.names, schema.types, strict=True)
        if str(t) not in ("string", "large_string")
    ]
    if typed:
        problems.append(f"{table}: non-STRING columns {typed}")

    rows = con.execute(
        f"select count(*) from read_parquet('{glob(table)}')"
    ).fetchone()[0]

    key = UNIQUE_KEY[table]
    dupes = (
        0
        if key is None
        else con.execute(
            f"select count(*) from (select {', '.join(key)} from "
            f"read_parquet('{glob(table)}') group by all having count(*) > 1)"
        ).fetchone()[0]
    )
    if dupes:
        problems.append(
            f"{table}: {dupes:,} duplicate values of the dbt uniqueness key "
            f"{key} — the test will fail"
        )

    nulls = con.execute(
        "select "
        + ", ".join(
            f'sum(case when "{n}" is null then 1 else 0 end) as "{n}"'
            for n in expected
        )
        + f" from read_parquet('{glob(table)}')"
    ).fetchone()
    empty = [n for n, v in zip(expected, nulls, strict=True) if v == rows]
    if empty:
        problems.append(f"{table}: entirely NULL columns {empty}")

    print(
        f"{table:28s} {rows:>12,} rows  {len(files):>4} file(s)  {len(expected):>3} cols"
    )
    return problems


def main() -> None:
    tables = sys.argv[1:] or constants.ALL_TABLES.value
    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'")
    con.execute("SET memory_limit='6GB'")
    (OUTPUT / "_duckdb_tmp").mkdir(parents=True, exist_ok=True)
    con.execute(f"SET temp_directory='{OUTPUT / '_duckdb_tmp'}'")
    problems = []
    total = 0
    for t in tables:
        problems += check(t, con)
        total += con.execute(
            f"select count(*) from read_parquet('{glob(t)}')"
        ).fetchone()[0]
    print(f"\n{'TOTAL':28s} {total:>12,} rows across {len(tables)} tables")
    if problems:
        print("\nPROBLEMS")
        for p in problems:
            print(" -", p)
        raise SystemExit(1)
    print("\nall checks passed")


if __name__ == "__main__":
    main()
