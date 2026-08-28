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

# Enough of the file to settle the dialect without reading gigabytes.
PROBE_BYTES = 4 << 20

KIND_TABLE = {"despesa": "pe_despesa", "pagamento": "pe_pagamento"}


def probe(path: Path) -> tuple[str, str]:
    """Detect this file's encoding and delimiter by reading it, not by guessing.

    PE's export is not internally consistent -- across the 19 exercises the encoding, the
    line ending and the separator all vary, with no pattern that tracks the year:

        2008 latin-1 ';'    2009 latin-1 ','    2010 latin-1 ';'
        2011 utf-8   ';'    2012 utf-8   ','    2015 utf-8   ';'
        2018 utf-8   ','    2019+ utf-8  ';'

    Two traps, both of which produced a wrong answer here before this was written properly:

    * **Do not test UTF-8 on a fixed-size slice.** Cutting at an arbitrary byte offset can
      split a multi-byte character, so the decode fails and a perfectly good UTF-8 file
      (2015) is misread as latin-1. An incremental decoder is fed the chunk and only
      `final=True` at real EOF, so a partial trailing sequence is not an error.
    * **Python's latin-1 decodes ANY byte sequence**, so "try utf-8, else latin-1" always
      answers latin-1 on failure without establishing anything. duckdb's latin-1 is
      stricter and rejects what Python accepts, which surfaces as
      `Invalid Input Error: File is not latin-1 encoded` -- an error about the *reader*,
      not the file. UTF-8 is therefore tested positively and latin-1 used only as the
      residual.
    """
    import codecs

    decoder = codecs.getincrementaldecoder("utf-8")()
    is_utf8 = True
    with path.open("rb") as fh:
        chunk = fh.read(PROBE_BYTES)
        at_eof = len(chunk) < PROBE_BYTES
        try:
            decoder.decode(chunk, final=at_eof)
        except UnicodeDecodeError:
            is_utf8 = False
    encoding = "utf-8" if is_utf8 else "latin-1"

    # The delimiter is whichever candidate dominates the header line.
    first = chunk.split(b"\n", 1)[0]
    delimiter = ";" if first.count(b";") >= first.count(b",") else ","
    return encoding, delimiter


def _relation(path: Path) -> str:
    # Types are forced to VARCHAR so the source's own number format survives intact; the
    # per-state normalisation happens in dbt, where it is visible.
    encoding, delimiter = probe(path)
    return (
        f"read_csv('{path}', header=true, all_varchar=true, delim='{delimiter}', "
        f"encoding='{encoding}', quote='\"', escape='\"', ignore_errors=false, "
        "union_by_name=true)"
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
