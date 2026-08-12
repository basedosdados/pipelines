"""Clean the INSEE SIRENE stock parquet files into typed Data Basis tables.

Out-of-core cleaning with DuckDB (the inputs are 30M-96M rows). One table at a
time, streamed straight from source parquet to a single Snappy parquet via COPY.

Column mapping, order, and types are authoritative in ``schema_map.py`` — this
module only generates the DuckDB SELECT from it; it never hand-transcribes names.

Casting rules (by target_type):
  STRING  -> NULLIF(TRIM(CAST(<src> AS VARCHAR)), '')   (empty string -> NULL)
  DATE    -> TRY_CAST(<src> AS DATE)
  INT64   -> TRY_CAST(<src> AS BIGINT)
  FLOAT64 -> TRY_CAST(<src> AS DOUBLE)
  data (__DATA__) -> DATE literal SNAPSHOT_DATE
  ano  (__ANO__)  -> SNAPSHOT_YEAR (BIGINT)

``geometria`` is NOT built here — the dbt model constructs it from lon/lat.
"""

import os
import sys

import duckdb

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from schema_map import (
    OUTPUT,
    SNAPSHOT_DATE,
    SNAPSHOT_YEAR,
    TABLES,
)

OUT_DIR = os.path.expanduser(OUTPUT)
TMP_DIR = os.path.expanduser("~/Downloads/fr_insee_sirene_data/duckdb_tmp")


def cast_expr(target_name: str, source_name: str, target_type: str) -> str:
    """Return a `<cast> AS <target_name>` SQL fragment for one column."""
    if source_name == "__DATA__":
        expr = f"DATE '{SNAPSHOT_DATE}'"
    elif source_name == "__ANO__":
        expr = f"CAST({SNAPSHOT_YEAR} AS BIGINT)"
    elif target_type == "STRING":
        expr = f"NULLIF(TRIM(CAST({source_name} AS VARCHAR)), '')"
    elif target_type == "DATE":
        expr = f"TRY_CAST({source_name} AS DATE)"
    elif target_type == "INT64":
        expr = f"TRY_CAST({source_name} AS BIGINT)"
    elif target_type == "FLOAT64":
        expr = f"TRY_CAST({source_name} AS DOUBLE)"
    else:
        raise ValueError(
            f"Unknown target_type {target_type!r} for {target_name}"
        )
    return f'{expr} AS "{target_name}"'


def build_query(spec: dict) -> str:
    """Build the full SELECT (with the geoloc LEFT JOIN for etablissement)."""
    select_list = ",\n    ".join(
        cast_expr(t, s, ty) for (t, s, ty) in spec["columns"]
    )
    src = os.path.expanduser(spec["src_file"])
    if spec.get("geoloc_file"):
        geo = os.path.expanduser(spec["geoloc_file"])
        key = spec["join_key"]
        # geoloc siret verified unique (37,575,264 distinct == total), so the
        # LEFT JOIN cannot fan out; row count stays equal to the stock file.
        return (
            f"SELECT\n    {select_list}\n"
            f"FROM read_parquet('{src}') AS s\n"
            f"LEFT JOIN read_parquet('{geo}') AS g ON s.{key} = g.{key}"
        )
    return f"SELECT\n    {select_list}\nFROM read_parquet('{src}')"


def clean_table(con: duckdb.DuckDBPyConnection, name: str, spec: dict) -> None:
    out_dir = os.path.join(OUT_DIR, name)
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "data.parquet")
    query = build_query(spec)

    print(f"\n=== {name} ===")
    con.execute(
        f"COPY ({query}) TO '{out_path}' (FORMAT PARQUET, COMPRESSION SNAPPY);"
    )

    n = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{out_path}')"
    ).fetchone()[0]
    expected = spec["expected_rows"]
    status = "OK" if n == expected else "MISMATCH"
    print(f"rows written: {n:,}  expected: {expected:,}  [{status}]")
    assert n == expected, f"{name}: row count {n} != expected {expected}"

    out_cols = [
        r[0]
        for r in con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{out_path}')"
        ).fetchall()
    ]
    want_cols = [t for (t, _s, _ty) in spec["columns"]]
    assert out_cols == want_cols, (
        f"{name}: column order mismatch\n got: {out_cols}\n want: {want_cols}"
    )
    print(f"columns: {len(out_cols)} in exact schema_map order  [OK]")
    print(f"path: {out_path}")


def main() -> None:
    os.makedirs(OUT_DIR, exist_ok=True)
    os.makedirs(TMP_DIR, exist_ok=True)
    con = duckdb.connect()
    con.execute("PRAGMA memory_limit='8GB';")
    con.execute(f"PRAGMA temp_directory='{TMP_DIR}';")
    con.execute("PRAGMA threads=4;")

    for name, spec in TABLES.items():
        clean_table(con, name, spec)

    print("\nAll tables cleaned successfully.")


if __name__ == "__main__":
    main()
