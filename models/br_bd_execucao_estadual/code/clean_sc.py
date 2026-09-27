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


def _rows(source, escapechar):
    """A CSV reader over `source` using one escaping convention."""
    return csv.reader(
        source,
        delimiter=SC_SEP,
        quotechar='"',
        doublequote=True,
        escapechar=escapechar,
    )


def _reparse(raw_record: str, escapechar):
    try:
        return next(_rows(io.StringIO(raw_record, newline=""), escapechar))
    except (csv.Error, StopIteration):
        return None


ESCAPE_CONVENTIONS: tuple = (None, "\\")


def parse_records(text: str) -> tuple[list[list[str]], object]:
    """Parse one export, choosing the escaping convention PER FILE.

    **SC uses a backslash as an escape character in some files and as literal data in
    others, and the choice cannot be made globally or per record.** Measured:

        liquidacao_201104   escapechar=None  0 bad   escapechar='\\'  1 bad
        liquidacao_201106   escapechar=None  0 bad   escapechar='\\'  1 bad
        liquidacao_202402   escapechar=None  2 bad   escapechar='\\'  0 bad

    `liquidacao_201106` carries the document number `"3932532\\"`, where the backslash
    is the value's last character and the quote after it closes the field.
    `liquidacao_202402` carries `\\"a empresa ...\\"` inside a free-text field that also
    contains a real newline -- under the wrong convention that ONE record splits into
    TWO (82,095 instead of 82,094), which is why a per-record repair cannot fix it: the
    record boundary itself is wrong.

    So both conventions are tried over the whole file and the one that places every
    record wins. Ties and total failures fall through to `_recover`, which resolves a
    single stubborn record structurally.
    """
    best: tuple[list[list[str]], object] | None = None
    best_bad = None
    for escapechar in ESCAPE_CONVENTIONS:
        try:
            rows = list(_rows(io.StringIO(text, newline=""), escapechar))
        except csv.Error:
            continue
        if not rows:
            continue
        width = len(rows[0])
        bad = sum(1 for r in rows[1:] if r and len(r) != width)
        if best_bad is None or bad < best_bad:
            best, best_bad = (rows, escapechar), bad
        if bad == 0:
            break
    if best is None:
        raise SystemExit("no escaping convention could parse the file at all")
    return best


def _raw_header(text: str) -> list[str]:
    """Column names exactly as the export writes them."""
    return next(_rows(io.StringIO(text, newline=""), None))


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

    A record the primary convention cannot place is re-read by `_recover` rather than
    dropped or forced. Anything `_recover` cannot resolve is a hard failure.
    """
    for out_sep in OUT_SEP_CANDIDATES:
        if out_sep in text:
            continue
        rows = 0
        repaired = 0
        with dest.open("w", encoding="utf-8", newline="") as fh:
            writer = csv.writer(
                fh,
                delimiter=out_sep,
                quoting=csv.QUOTE_NONE,
                # quotechar must be a 1-char string or None; "" raises TypeError. None
                # is how QUOTE_NONE disables quoting. out_sep is verified absent from
                # the text above, so no field ever needs escaping (escapechar stays None).
                quotechar=None,
                escapechar=None,
            )
            records, escapechar = parse_records(text)
            for i, row in enumerate(records):
                if i == 0:
                    writer.writerow(header)
                    continue
                if not row:
                    continue
                if len(row) != len(header):
                    row = _recover(
                        SC_SEP.join(row), header, dest.name, i, escapechar
                    )
                    repaired += 1
                # Real newlines inside free-text fields survive the quote-aware parse
                # and would become row breaks in an unquoted file.
                writer.writerow(
                    [c.replace("\r", " ").replace("\n", " ") for c in row]
                )
                rows += 1
        if repaired:
            print(
                f"    {dest.name}: {repaired} row(s) repaired as literal quotes",
                flush=True,
            )
        return out_sep, rows
    raise SystemExit(
        f"{dest.name}: every candidate delimiter occurs in the source"
    )


def _recover(
    raw_record: str,
    header: list[str],
    name: str,
    index: int,
    escapechar: object,
) -> list[str]:
    """Place one record the file's chosen convention still could not.

    `parse_records` has already picked whichever escaping convention places every other
    record, so reaching here means one record is malformed under both. The last resort
    is BA's: trust the unambiguous `;` separator rather than the quoting.

    It is accepted only when it yields the expected width AND leaves the final column
    numeric. That second test is what separates this from `strict_mode=false`, which
    mis-parses silently -- a repair that shifted the fields would fail it.
    """
    fields = [f.strip('"') for f in raw_record.rstrip("\r\n").split(SC_SEP)]
    if len(fields) == len(header):
        try:
            float(fields[-1].replace(",", ".").strip())
        except ValueError:
            raise SystemExit(
                f"{name}: row {index} split to {len(fields)} fields but its last column "
                f"is {fields[-1]!r}, not a number -- the fields are shifted."
            ) from None
        return fields

    raise SystemExit(
        f"{name}: row {index} could not be placed under escapechar={escapechar!r} "
        f"(structural split gives {len(fields)}, expected {len(header)}). The export's "
        f"shape has changed; measure it before relaxing anything."
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
