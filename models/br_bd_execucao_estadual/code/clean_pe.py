"""Convert Pernambuco source files to all-STRING staging parquet.

PE publishes one denormalised CSV per exercise off e-Fisco -- no dimensional model, no
lookup tables. Each row already carries the empenho number and every classification as a
`"<code> - <label>"` string (`"06 - SEGURANÇA PÚBLICA"`), so the split into code and label
happens in dbt rather than here, and staging stays a faithful mirror.

Same all-STRING rule as MG: see clean_mg.py for why, and .claude/rules for the convention.
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent))
from constants import INPUT_DIR, OUTPUT_DIR

PE_INPUT = INPUT_DIR / "pe"

# despesa_2023.csv / pagamento_2023.csv, written by download_pe.py
NAME_RE = re.compile(r"^(?P<kind>despesa|pagamento)_(?P<year>\d{4})\.csv$")

KIND_TABLE = {"despesa": "pe_despesa", "pagamento": "pe_pagamento"}


def _relation(path: Path) -> str:
    # PE quotes most fields and uses ';'-free commas; the sniffer gets the dialect right,
    # but the types must still be forced to VARCHAR so BR-formatted values survive intact.
    return (
        f"read_csv('{path}', header=true, all_varchar=true, "
        "quote='\"', escape='\"', ignore_errors=false, union_by_name=true)"
    )


def clean(
    con: duckdb.DuckDBPyConnection, kind: str, only_year: int | None
) -> int:
    table = KIND_TABLE[kind]
    dest = OUTPUT_DIR / table
    dest.mkdir(parents=True, exist_ok=True)

    files = []
    for path in sorted(PE_INPUT.glob(f"{kind}_*.csv")):
        m = NAME_RE.match(path.name)
        if m and (only_year is None or int(m.group("year")) == only_year):
            files.append((int(m.group("year")), path))
    if not files:
        print(f"  SKIP {table}: nothing downloaded")
        return 0

    total = 0
    for year, path in files:
        out = dest / f"data_{year}.parquet"
        # `ano` is stamped from the file rather than read from the data: PE's expense rows
        # carry `numero_empenho` like "2018NE000122", whose year is the empenho's own
        # exercise and differs from the file's exercise for restos a pagar. The file is the
        # authority for which exercise the row was published under; the empenho year is
        # recovered separately in dbt.
        con.execute(
            f"COPY (SELECT {year} AS ano, * FROM {_relation(path)}) "
            f"TO '{out}' (FORMAT PARQUET, COMPRESSION SNAPPY)"
        )
        n = con.execute(
            f"SELECT count(*) FROM read_parquet('{out}')"
        ).fetchone()[0]
        total += n
        print(f"    {path.name}: {n:,} rows", flush=True)
    print(f"  {table}: {total:,} rows across {len(files)} files")
    return total


def main(kind: str | None = None, year: int | None = None) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute("SET preserve_insertion_order=false")
    con.execute("PRAGMA memory_limit='6GB'")
    for k in KIND_TABLE:
        if kind and k != kind:
            continue
        clean(con, k, year)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--kind", choices=sorted(KIND_TABLE))
    ap.add_argument("--year", type=int)
    main(**vars(ap.parse_args()))
