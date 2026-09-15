"""Convert Santa Catarina source files to all-STRING staging parquet.

SC arrives as one CSV per (visão, month) from the portal's export endpoint -- see
`download_sc.py` for why the CKAN bulk files are not used. Three staging tables come
out, one per phase: `sc_empenho`, `sc_liquidacao`, `sc_pagamento`.

The export IS quoted, unlike the CKAN bulk files -- but not consistently, so it still
cannot be handed to duckdb directly. Three source defects are handled here:

* **Encoding.** cp1252 while the `Content-Type` claims UTF-8, and duckdb has no cp1252
  reader. See `_decode`.
* **Two quote-escaping conventions, mixed.** An embedded double quote is sometimes
  doubled (`""`) and sometimes backslashed (`\\"`), occasionally in the same file. No
  single duckdb setting reads both. See `_reader` and `_reemit`.
* **Real newlines inside quoted free-text fields**, which survive the parse and would
  become row breaks in an unquoted re-emit. See `_reemit`.

One thing genuinely is simpler than RS: **there is no schema drift.** All 36 empenho
columns are byte-identical from 2011-01 to 2026, where RS drifts three times and PE has
three disjoint eras. The reference header is still asserted per file, because a silent
`union_by_name` over a changed header is the PE failure (1,031,326 rows present and
entirely NULL).

Every month is checked twice: the re-emitted row count must equal what duckdb reads,
and both must equal the row count `download_sc` verified against the portal's own
published total.

Staging is all-STRING by house convention, and it must be all-STRING here specifically:
the recurring-pipeline upload path stringifies its header, so a typed external table
left behind by onboarding collides with the pipeline's later overwrite. See
.claude/rules/prefect-pipeline-conventions.md, "Staging parquet must be all-STRING".
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import sys
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent))
from constants import (
    INPUT_DIR,
    OUTPUT_DIR,
    SC_ENCODING,
    SC_SEP,
    SC_TABLES,
    SC_VISOES,
    normalise_column,
)

SC_INPUT = INPUT_DIR / "sc"

# Control characters, in the order tried. The chosen one must not occur anywhere
# in the file -- see `_reemit`.
OUT_SEP_CANDIDATES = ("\x1f", "\x1e", "\x02", "\x03", "\x04", "\x1d", "\x01")


class HeaderDriftError(Exception):
    """A month's header differs from the visão's reference header."""


def _decode(raw: bytes) -> str:
    """Decode one export, tolerating either encoding the file may be in.

    The portal serves **cp1252** while its `Content-Type` claims UTF-8. But a file may
    already have been transcoded on the way in, so UTF-8 is tried first.

    **"utf-8 strict, else cp1252" is a legitimate probe; "utf-8, else latin-1" is not.**
    UTF-8 is self-validating, and the accented bytes cp1252 uses are lone high bytes
    that are invalid UTF-8 -- so a Portuguese-language cp1252 file cannot pass as UTF-8
    by accident. latin-1, by contrast, decodes ANY byte sequence, so it can never
    disprove anything and would report success on mojibake. That is the RS lesson.

    The five bytes cp1252 itself leaves undefined (0x81, 0x8D, 0x8F, 0x90, 0x9D) fall
    back to their latin-1 characters, which is lossless, rather than to U+FFFD.
    """
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        pass
    try:
        return raw.decode(SC_ENCODING)
    except UnicodeDecodeError:
        return "".join(
            bytes([b]).decode(SC_ENCODING, errors="ignore")
            or bytes([b]).decode("latin-1")
            for b in raw
        )


def _reader(text: str):
    """A reader that accepts BOTH quote-escaping conventions the export mixes.

    SC escapes an embedded double quote two different ways, sometimes in the same file:
    doubled (`""`, the CSV standard) and backslashed (`\\"`). 2011-01 carries 7 of the
    second and 5 of the first; 2013-06 has 55 doubled and no backslashes; 2016-01 has
    neither. No single duckdb setting reads both -- `escape='"'` dies on the backslash
    form with `Value with unterminated quote found`, and `escape='\\'` would die on the
    doubled form.

    Python's csv module does support both at once, so the parse happens here and the
    rows are handed to duckdb in a format with no quoting at all (see `clean_month`).
    """
    return csv.reader(
        io.StringIO(text, newline=""),
        delimiter=SC_SEP,
        quotechar='"',
        doublequote=True,
        escapechar="\\",
    )


def _raw_header(text: str) -> list[str]:
    """Column names exactly as the export writes them."""
    return next(_reader(text))


def _header_of(text: str) -> list[str]:
    """Column names folded to the house convention.

    A no-op on today's export -- every name is already lowercase ASCII -- but SC is not
    the source of truth for that, and BigQuery rejects spaces, accents and leading
    digits outright.
    """
    return [normalise_column(c) for c in _raw_header(text)]


def _reemit(text: str, dest: Path, header: list[str]) -> tuple[str, int]:
    """Rewrite one export with a delimiter absent from the data and no quoting.

    Rejoining fields with the original `;` would put the separator back INTO the data
    with no way to say "this one is data" -- the repair that silently did nothing on
    RS. Emitting with a delimiter that does not occur anywhere in the file removes
    quoting from the problem instead of restating it.

    The delimiter is chosen per file from a candidate list and the choice is *verified*
    against the whole text, not sampled: a control byte appearing as data is exactly
    what RS's 2016-02 file turned out to contain.

    Rows whose field count differs from the header are a hard failure, not a repair:
    unlike RS, where the surplus provably belonged to the last column, nothing here
    establishes where a stray field came from.
    """
    for out_sep in OUT_SEP_CANDIDATES:
        if out_sep in text:
            continue
        rows = 0
        with dest.open("w", encoding="utf-8", newline="") as fh:
            writer = csv.writer(
                fh,
                delimiter=out_sep,
                quoting=csv.QUOTE_NONE,
                quotechar="",
                escapechar=None,
            )
            for i, row in enumerate(_reader(text)):
                if i == 0:
                    writer.writerow(header)
                    continue
                if not row:
                    continue
                if len(row) != len(header):
                    raise SystemExit(
                        f"{dest.name}: row {i} has {len(row)} fields, expected "
                        f"{len(header)}. The export's quoting has changed shape; "
                        f"measure it before relaxing anything."
                    )
                # Real newlines inside free-text fields survive the quote-aware parse
                # and would become row breaks in an unquoted file.
                writer.writerow(
                    [c.replace("\r", " ").replace("\n", " ") for c in row]
                )
                rows += 1
        return out_sep, rows
    raise SystemExit(
        f"{dest.name}: every candidate delimiter occurs in the source"
    )


def _recorded_rows(source: Path) -> int | None:
    """Row count `download_sc` verified against the portal's own published total."""
    meta = source.with_suffix(".json")
    if not meta.exists():
        return None
    return json.loads(meta.read_text()).get("rows")


def clean_month(
    con: duckdb.DuckDBPyConnection,
    source: Path,
    dest_dir: Path,
    reference: list[str] | None,
) -> tuple[int, list[str]]:
    raw = source.read_bytes()
    text = _decode(raw)
    header = _header_of(text)

    if reference is not None and header != reference:
        missing = [c for c in reference if c not in header]
        extra = [c for c in header if c not in reference]
        raise HeaderDriftError(
            f"{source.name}: header differs from the reference "
            f"({len(header)} vs {len(reference)} columns; missing={missing}; extra={extra}). "
            f"Unioning these silently would leave one schema's columns NULL for every "
            f"row of the other -- resolve deliberately before continuing."
        )

    tmp = source.with_suffix(".utf8.tmp")
    dest_dir.mkdir(parents=True, exist_ok=True)
    out_path = dest_dir / f"data_{source.stem.rsplit('_', 1)[-1]}.parquet"
    try:
        out_sep, written = _reemit(text, tmp, header)
        # The temp file uses a control character as its delimiter and no quoting at
        # all, so `"`, `\` and `;` are ordinary data by construction and duckdb has
        # nothing left to misread.
        rel = (
            f"read_csv('{tmp}', delim='{out_sep}', header=true, all_varchar=true, "
            f"quote='', encoding='utf-8', ignore_errors=false, sample_size=-1)"
        )
        con.execute(
            f"COPY (SELECT * FROM {rel}) TO '{out_path}' "
            f"(FORMAT PARQUET, COMPRESSION SNAPPY)"
        )
        n = con.execute(
            f"SELECT count(*) FROM read_parquet('{out_path}')"
        ).fetchone()[0]
        if n != written:
            out_path.unlink(missing_ok=True)
            raise SystemExit(
                f"{source.name}: re-emitted {written:,} rows but duckdb read {n:,}"
            )
    finally:
        tmp.unlink(missing_ok=True)

    want = _recorded_rows(source)
    if want is not None and n != want:
        out_path.unlink(missing_ok=True)
        raise SystemExit(
            f"{source.name}: parquet has {n:,} rows but the portal published {want:,}. "
            f"The quoting assumption has broken for this month -- do NOT relax it with "
            f"strict_mode=false, which mis-parses rather than rejects."
        )

    if n == 0:
        # An empty first partition makes dump_header infer INTEGER for every column and
        # poisons the staging schema. See
        # reference_empty_parquet_partition_poisons_staging_schema.
        out_path.unlink()
        print(f"    {source.name}: 0 rows, parquet dropped")
        return 0, header

    print(f"    {source.name}: {n:,} rows", flush=True)
    return n, header


def main(
    visoes: tuple[str, ...] = SC_VISOES, only_year: int | None = None
) -> None:
    con = duckdb.connect()
    # The peak here is one month, but duckdb will happily take the whole machine.
    con.execute("SET memory_limit='2GB'")
    grand: dict[str, int] = {}

    for visao in visoes:
        table = SC_TABLES[visao]
        sources = sorted((SC_INPUT / visao).glob(f"{visao}_*.csv"))
        if only_year is not None:
            sources = [
                p
                for p in sources
                if p.stem.rsplit("_", 1)[-1].startswith(str(only_year))
            ]
        if not sources:
            print(f"  {visao}: no input files found", flush=True)
            continue

        print(f"  {visao} -> {table} ({len(sources)} month(s))", flush=True)
        reference: list[str] | None = None
        total = 0
        for source in sources:
            n, header = clean_month(con, source, OUTPUT_DIR / table, reference)
            reference = reference or header
            total += n
        grand[table] = total
        print(f"  == {table}: {total:,} rows", flush=True)

    for table, n in grand.items():
        print(f"{table:<16} {n:>12,}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--visao", choices=SC_VISOES, action="append")
    parser.add_argument("--year", type=int)
    args = parser.parse_args()
    main(
        visoes=tuple(args.visao) if args.visao else SC_VISOES,
        only_year=args.year,
    )
