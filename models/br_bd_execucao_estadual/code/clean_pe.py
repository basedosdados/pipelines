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
from constants import INPUT_DIR, OUTPUT_DIR, normalise_column

PE_INPUT = INPUT_DIR / "pe"

# despesa_2023.csv / pagamento_2023.csv, written by download_pe.py
NAME_RE = re.compile(r"^(?P<kind>despesa|pagamento)_(?P<year>\d{4})\.csv$")

# Enough of the file to settle the dialect without reading gigabytes.
PROBE_BYTES = 4 << 20

KIND_TABLE = {"despesa": "pe_despesa", "pagamento": "pe_pagamento"}

# A column that exists ONLY in the modern export. PE rebuilt its schema twice and the
# column names share almost nothing across eras, so the eras cannot share a staging table:
# BigQuery infers ONE schema for a wildcard parquet load and silently keeps the columns of
# whichever file it resolves to, loading the other era's rows as all-NULL. It reports the
# full row count while doing it, so nothing looks wrong. Splitting the eras into separate
# tables makes each schema self-consistent and the load lossless.
MODERN_MARKER = "numero_empenho"
LEGACY_SUFFIX = "_legado"


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


def header_of(path: Path) -> list[str]:
    encoding, delimiter = probe(path)
    with path.open(encoding=encoding, newline="") as fh:
        line = fh.readline().rstrip("\r\n")
    return [c.strip().strip('"') for c in line.split(delimiter)]


def _column_names(path: Path) -> list[str]:
    """Normalised, de-duplicated column names for one file.

    Two collisions have to be resolved. The 2008 export already carries its own `Ano`
    column, which would clash with the exercise this step stamps on every row -- so a
    source column normalising to `ano` becomes `ano_fonte`, keeping the stamped value
    authoritative and the published one visible. Any other repeat gets a numeric suffix
    rather than silently overwriting its twin.
    """
    seen: dict[str, int] = {}
    out: list[str] = []
    for raw in header_of(path):
        name = normalise_column(raw)
        if name == "ano":
            name = "ano_fonte"
        if name in seen:
            seen[name] += 1
            name = f"{name}_{seen[name]}"
        else:
            seen[name] = 0
        out.append(name)
    return out


def _relation(path: Path) -> str:
    """Read one PE file with its own dialect, under BigQuery-legal column names.

    The legacy export's headings are human text -- "Cod. Acao", "13.02 - Razao Social" --
    and BigQuery refuses both: a '.' in a parquet field name fails the load outright with
    `Character '.' found in field name`, and a name cannot start with a digit. Renaming at
    read time keeps the rest of the pipeline from having to quote them.

    Types are forced to VARCHAR so the source's own number format survives intact; the
    per-state normalisation happens in dbt, where it is visible.
    """
    encoding, delimiter = probe(path)
    cols = ", ".join(f"'{k}': 'VARCHAR'" for k in _column_names(path))
    return (
        f"read_csv('{path}', header=true, auto_detect=false, delim='{delimiter}', "
        f"encoding='{encoding}', quote='\"', escape='\"', ignore_errors=false, "
        f"columns={{{cols}}})"
    )


def harmonise(
    con: duckdb.DuckDBPyConnection, d: Path, parts: list[Path]
) -> None:
    """Give every file in one staging table the same columns.

    Splitting PE by era is not enough on its own: the legacy era is itself two schemas
    (2008 has 40 source columns, 2009-2010 have 46), and a wildcard parquet load infers
    ONE schema -- so BigQuery would keep one file's columns and load the others' rows as
    all-NULL while reporting the full row count. Rewriting each file from a union-by-name
    relation gives them a common superset, with genuine NULLs where a column did not
    exist in that exercise.
    """
    widths = {
        len(
            con.execute(
                f"DESCRIBE SELECT * FROM read_parquet('{p}')"
            ).fetchall()
        )
        for p in parts
    }
    if len(widths) <= 1:
        return
    print(
        f"    harmonising {d.name}: widths {sorted(widths)} -> union",
        flush=True,
    )
    # One PE file is one exercise, so `ano` identifies the file and no filename column is
    # needed to route rows back to it.
    for part in parts:
        year = part.stem.split("_")[-1]
        tmp = part.with_suffix(".harmonised")
        con.execute(
            f"COPY (SELECT * FROM read_parquet('{d}/data_*.parquet', "
            f"                                 union_by_name=true) "
            f"      WHERE ano = {year}) "
            f"TO '{tmp}' (FORMAT PARQUET, COMPRESSION SNAPPY)"
        )
    for part in parts:
        part.with_suffix(".harmonised").replace(part)


def era_of(path: Path) -> str:
    """ "modern" or "legacy", decided by the header rather than by the year.

    Using the header means a re-publication that moves the boundary -- or an old exercise
    reissued in the new format -- lands in the right table on its own.
    """
    encoding, delimiter = probe(path)
    with path.open(encoding=encoding, newline="") as fh:
        header = fh.readline()
    cols = {c.strip().strip('"').lower() for c in header.split(delimiter)}
    return "modern" if MODERN_MARKER in cols else "legacy"


def clean(
    con: duckdb.DuckDBPyConnection, kind: str, only_year: int | None
) -> int:
    base = KIND_TABLE[kind]
    # Clear both era directories: a file that changes era between runs would otherwise
    # leave a stale copy behind and be counted twice.
    for suffix in ("", LEGACY_SUFFIX):
        stale_dir = OUTPUT_DIR / f"{base}{suffix}"
        if stale_dir.exists():
            for stale in stale_dir.glob("*.parquet"):
                stale.unlink()

    files = []
    for path in sorted(PE_INPUT.glob(f"{kind}_*.csv")):
        m = NAME_RE.match(path.name)
        if m and (only_year is None or int(m.group("year")) == only_year):
            files.append((int(m.group("year")), path))
    if not files:
        print(f"  SKIP {base}: nothing downloaded")
        return 0

    total = 0
    for year, path in files:
        era = era_of(path)
        dest = OUTPUT_DIR / (base if era == "modern" else base + LEGACY_SUFFIX)
        dest.mkdir(parents=True, exist_ok=True)
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
        ncols = len(
            con.execute(
                f"DESCRIBE SELECT * FROM read_parquet('{out}')"
            ).fetchall()
        )
        total += n
        enc, delim = probe(path)
        # A wrong delimiter yields one data column and no error, so the width is reported
        # per file rather than assumed.
        flag = "  <-- SUSPECT single column" if ncols <= 2 else ""
        print(
            f"    {path.name}: {n:,} rows, {ncols} cols "
            f"({enc}, {delim!r}, {era}){flag}",
            flush=True,
        )

    # Report each era's table separately: the whole point of the split is that they do not
    # share a schema, so a single combined count would hide a lopsided or empty era.
    for suffix in ("", LEGACY_SUFFIX):
        d = OUTPUT_DIR / f"{base}{suffix}"
        parts = sorted(d.glob("*.parquet")) if d.exists() else []
        if not parts:
            continue
        harmonise(con, d, parts)
        parts = sorted(d.glob("*.parquet"))
        rows = con.execute(
            f"SELECT count(*) FROM read_parquet('{d}/*.parquet')"
        ).fetchone()[0]
        widths = {
            len(
                con.execute(
                    f"DESCRIBE SELECT * FROM read_parquet('{p}')"
                ).fetchall()
            )
            for p in parts
        }
        print(
            f"  {base}{suffix}: {rows:,} rows, {len(parts)} files, "
            f"column widths {sorted(widths)}"
        )
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
