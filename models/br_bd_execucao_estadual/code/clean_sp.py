"""Convert the scraped São Paulo SIGEO exports to all-STRING staging parquet.

One CSV per (exercise, órgão), as written by download_sp.py. The files are latin-1, have a
trailing empty field from a line-ending comma, and share a fixed nine-column header:

    Órgão, Unidade Gestora, Fonte de Recursos, Credor, Despesa,
    Empenhado, Liquidado, Pago, Pago Restos

Every column arrives as "<code> - <label>", and the year comes from the file name because
the export does not carry it.

**Number format is per-state and SP's is a third variant.** SP writes
`"           2.693.456,58"` -- leading spaces, `.` as thousands separator, `,` as decimal.
BA writes `2643000,00` (comma decimal, NO thousands separator) and PE writes ` 43200.0`
(plain US). Applying BA's rule to SP turns 2.693.456,58 into 2.693.456.58, which safe_cast
silently returns as NULL: 76% of SP's values carry a thousands separator, so that mistake
would empty three quarters of the table without raising. The strings are therefore left
untouched here -- staging is a faithful mirror -- and normalised once, per state, in dbt.
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent))
from constants import INPUT_DIR, OUTPUT_DIR

SP_INPUT = INPUT_DIR / "sp"
TABLE = "sp_despesa"

# despesa_2023_29000.csv -> (2023, "29000")
NAME_RE = re.compile(r"^despesa_(?P<year>\d{4})_(?P<orgao>\w+)\.csv$")

# Declared rather than sniffed: the trailing comma on every line yields a tenth, always
# empty field, which a sniffer reports as a ragged row.
COLUMNS = {
    "orgao": "VARCHAR",
    "unidade_gestora": "VARCHAR",
    "fonte_recurso": "VARCHAR",
    "credor": "VARCHAR",
    "despesa": "VARCHAR",
    "empenhado": "VARCHAR",
    "liquidado": "VARCHAR",
    "pago": "VARCHAR",
    "pago_restos": "VARCHAR",
    "_trailing": "VARCHAR",
}


def _relation(paths: str) -> str:
    cols = ", ".join(f"'{k}': '{v}'" for k, v in COLUMNS.items())
    return (
        # cp1252, NOT latin-1. 162 of the 509 exports carry bytes in 0x80-0x9F -- the
        # curly apostrophe in "MA PLACAS ART'S E GRAVACOES" is 0x92 -- which are
        # undefined in strict latin-1, and duckdb rejects the file outright rather than
        # substituting. Note duckdb spells it `cp1252`; `windows-1252` is not accepted.
        # `utf-8` would be accepted here and is WRONG: duckdb passes the bytes through
        # unvalidated, silently mangling every accented character in the file.
        f"read_csv('{paths}', header=true, auto_detect=false, encoding='cp1252', "
        f"quote='\"', escape='\"', ignore_errors=false, columns={{{cols}}}, "
        "filename=true)"
    )


def clean(con: duckdb.DuckDBPyConnection) -> int:
    files = sorted(SP_INPUT.glob("despesa_*.csv"))
    if not files:
        print("  SKIP sp_despesa: nothing scraped yet")
        return 0
    years = sorted(
        {
            NAME_RE.match(p.name).group("year")
            for p in files
            if NAME_RE.match(p.name)
        }
    )

    dest = OUTPUT_DIR / TABLE
    dest.mkdir(parents=True, exist_ok=True)
    for stale in dest.glob("*.parquet"):
        stale.unlink()

    total = 0
    for year in years:
        rel = _relation(str(SP_INPUT / f"despesa_{year}_*.csv"))
        out = dest / f"data_{year}.parquet"
        # The exercise is not in the export, so it is stamped from the file name -- the
        # only place it exists. The órgão code is likewise recovered from the name, as a
        # cross-check against the Órgão column the query itself returned.
        con.execute(
            f"COPY (SELECT * EXCLUDE (_trailing, filename), "
            f"       '{year}' AS ano, "
            # duckdb has no r'' raw-string prefix -- that is BigQuery syntax. A plain
            # SQL string passes the backslashes through to the regex engine unchanged.
            f"       regexp_extract(filename, 'despesa_\\d{{4}}_(\\w+)\\.csv$', 1) "
            f"           AS orgao_arquivo "
            f"      FROM {rel}) "
            f"TO '{out}' (FORMAT PARQUET, COMPRESSION SNAPPY)"
        )
        n = con.execute(
            f"SELECT count(*) FROM read_parquet('{out}')"
        ).fetchone()[0]
        total += n
        print(f"    {year}: {n:,} rows", flush=True)
    print(
        f"  {TABLE}: {total:,} rows across {len(files)} files, {len(years)} exercises"
    )
    return total


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute("SET preserve_insertion_order=false")
    con.execute("PRAGMA memory_limit='6GB'")
    clean(con)


if __name__ == "__main__":
    argparse.ArgumentParser().parse_args()
    main()
