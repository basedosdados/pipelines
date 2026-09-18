"""Convert Rio Grande do Sul source files to all-STRING staging parquet.

RS publishes one flat table (CAGE's `Gasto-RS`) as twelve monthly ZIPs per exercise,
2012-2026 -- 175 archives, 52,224,456 rows. Each holds a single Windows-1252 CSV.

Each archive is expanded, repaired, converted, and its temporary CSV deleted before the
next is opened. The series is ~2.3 GB zipped but ~36 GB expanded, so extracting
everything up front would need more scratch than the other four states combined.

Three source defects are handled here, each measured rather than assumed: see
`_transcode` for the encoding and the ragged rows, and `superset_columns` for the
schema drift.

Staging is all-STRING by house convention, and it must be all-STRING here specifically:
the recurring-pipeline upload path stringifies its header, so a typed external table
left behind by onboarding collides with the pipeline's later overwrite. See
.claude/rules/prefect-pipeline-conventions.md, "Staging parquet must be all-STRING".
"""

from __future__ import annotations

import argparse
import codecs
import io
import sys
import zipfile
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent))
from constants import (
    INPUT_DIR,
    OUTPUT_DIR,
    RS_ENCODING,
    RS_SEP,
    RS_TABLE,
    normalise_column,
)

RS_INPUT = INPUT_DIR / "rs"

# `Exercicio` and `Mes` are carried INSIDE the file, so they are used rather than the
# file name -- the same rule as MG's `ano_particao`. `Exercicio` is renamed to the
# house name so the models and the coverage refresh find it where they expect.
RENAME = {"exercicio": "ano"}

# The five bytes cp1252 leaves undefined. RS's 2012 files carry a handful each.
CP1252_UNDEFINED = (b"\x81", b"\x8d", b"\x8f", b"\x90", b"\x9d")

# The temp file is written with SOH as its delimiter, not the source's semicolon.
#
# This is what makes the ragged-row repair expressible at all. The source is unquoted,
# so rejoining a split free-text field with `;` puts the separator straight back into
# the field and the row is still ragged -- the repair silently accomplishes nothing.
# There is no way to say "this semicolon is data" in a file with no quoting.
#
# Re-emitting with a delimiter that cannot appear in the data sidesteps quoting
# entirely: no escaping, no quote character for a stray `"` to trip over (Bahia's
# failure), and the repaired field keeps its semicolons verbatim.
#
# The delimiter is CHOSEN PER FILE rather than fixed, because RS's data does contain
# control bytes -- 2016-02 carries a literal 0x01. `_transcode` refuses to write a
# delimiter it has seen in the input, and `clean_archive` moves to the next candidate.
# Since the delimiter only ever describes the throwaway temp file, files may disagree
# about it without any effect on the parquet they produce.
#
# US (0x1F, "unit separator") is first because separating fields is precisely what it
# was defined for.
OUT_SEP_CANDIDATES = ("\x1f", "\x1e", "\x02", "\x03", "\x04", "\x1d", "\x01")


class DelimiterCollisionError(Exception):
    """The chosen output delimiter occurs in the source; try the next candidate."""


def _partition_of(archive: Path) -> str:
    """The YYYYMM this archive's file name claims."""
    return archive.stem.rsplit("-", 1)[-1]


def _member_size(archive: Path) -> int:
    with zipfile.ZipFile(archive) as z:
        return _sole_csv(z, archive).file_size


def _cp1252_fallback(exc: UnicodeDecodeError) -> tuple[str, int]:
    """Decode a byte cp1252 leaves undefined as its latin-1 character.

    Lossless and round-trippable, where `errors="replace"` would silently swap in
    U+FFFD and lose the byte.
    """
    return "".join(chr(b) for b in exc.object[exc.start : exc.end]), exc.end


codecs.register_error("rs_cp1252_fallback", _cp1252_fallback)


def _sole_csv(z: zipfile.ZipFile, archive: Path) -> zipfile.ZipInfo:
    members = [m for m in z.infolist() if m.filename.lower().endswith(".csv")]
    if len(members) != 1:
        raise SystemExit(
            f"{archive.name}: expected exactly one CSV, found "
            f"{[m.filename for m in members]}"
        )
    return members[0]


def _header_of(archive: Path) -> list[str]:
    with zipfile.ZipFile(archive) as z, z.open(_sole_csv(z, archive)) as fh:
        raw = fh.read(16000)
    text = raw.decode(RS_ENCODING, errors="rs_cp1252_fallback")
    return text.split("\n", 1)[0].rstrip("\r").split(RS_SEP)


def _normalised(columns: list[str]) -> list[str]:
    out: list[str] = []
    seen: dict[str, int] = {}
    for name in columns:
        norm = RENAME.get(normalise_column(name), normalise_column(name))
        # Defensive: two source names could fold to one legal name, and BigQuery would
        # reject the duplicate at load time rather than here.
        if norm in seen:
            seen[norm] += 1
            norm = f"{norm}_{seen[norm]}"
        else:
            seen[norm] = 0
        out.append(norm)
    return out


def superset_columns(archives: list[Path]) -> list[str]:
    """The union of every era's columns, in first-seen order.

    RS changed its schema three times: 62 columns for 2012-01..2025-05, 66 from 2025-06
    (adding the fonte/natureza code and name pairs), and 67 from 2025-07 (adding
    `Historico`, and dropping the accent from `Informações Complementares`).

    Every partition must be written with the SAME columns, or the upload silently loses
    the difference: a wildcard parquet load infers one schema, keeps those columns, and
    loads every other file's rows as all-NULL while reporting the full row count. That
    cost PE 25 columns and 1,031,326 rows before `upload.py` grew a guard against it.

    Normalisation is what folds the accent rename onto one name, so the two spellings
    do not become two columns.
    """
    seen: list[str] = []
    for archive in archives:
        for norm in _normalised(_header_of(archive)):
            if norm not in seen:
                seen.append(norm)
    return seen


def _transcode(
    z: zipfile.ZipFile, member: zipfile.ZipInfo, dest: Path, out_sep: str
) -> tuple[int, int]:
    """Unpack one CSV to UTF-8, repairing ragged rows. Returns (recovered, repaired).

    **Encoding.** The source is Windows-1252, not the ISO-8859-1 its accents suggest:
    it carries smart quotes and en-dashes in the C1 range (0x91-0x96) that latin-1
    leaves undefined, so duckdb refuses every file with `Invalid Input Error: File is
    not latin-1 encoded`. That refusal is correct and useful -- Python's latin-1
    decodes ANY byte sequence, so a "try utf-8, else latin-1" probe reports success and
    yields mojibake. duckdb has no cp1252 reader, so the conversion happens here, on a
    pass the unpacking was already paying for.

    **Ragged rows.** The source uses no quoting convention: the final column
    (`Informacoes Complementares`) is free text and its semicolons are left unescaped,
    e.g. `NFSe:4306; Comp:06/2025; CTR: 2023/20385`. 17,366 rows across the 13 files
    from 2025-07 carry up to 7 surplus fields. `strict_mode=false` does not fix this
    (it mis-parses rather than rejects) and `null_padding` only pads rows that are too
    SHORT.

    The surplus always belongs to the last column, which makes the repair unambiguous.
    Verified over every ragged row in the corpus by checking that the field preceding
    the free text is still the natureza code: **17,366 of 17,366 hold, 0 violate.**

    Quotes are left as ordinary characters. 1,619 rows contain one, but since embedded
    separators are demonstrably NOT quoted, `"` carries no structural meaning here --
    the reader is given `quote=''` so a stray unpaired quote cannot swallow the rest of
    the file, as one did in Bahia.
    """
    fields: int | None = None
    repaired = 0
    with z.open(member) as raw:
        stream = io.TextIOWrapper(
            raw, encoding=RS_ENCODING, errors="rs_cp1252_fallback", newline=""
        )
        with dest.open("w", encoding="utf-8", newline="") as out:
            for line in stream:
                line = line.rstrip("\r\n")
                if not line:
                    continue
                if out_sep in line:
                    raise DelimiterCollisionError(out_sep)
                parts = line.split(RS_SEP)
                if fields is None:
                    fields = len(parts)
                elif len(parts) > fields:
                    # Surplus fields belong to the trailing free-text column, verified
                    # over all 17,366 ragged rows in the corpus. Rejoined with the
                    # ORIGINAL separator so the text keeps its semicolons; that is only
                    # unambiguous because the output uses a different delimiter.
                    parts = [
                        *parts[: fields - 1],
                        RS_SEP.join(parts[fields - 1 :]),
                    ]
                    repaired += 1
                out.write(out_sep.join(parts) + "\n")

    recovered = 0
    with z.open(member) as raw:
        while chunk := raw.read(1 << 20):
            recovered += sum(chunk.count(b) for b in CP1252_UNDEFINED)
    return recovered, repaired


def _projection(columns: list[str], superset: list[str]) -> str:
    """Project one era's columns into the superset, NULL for the ones it lacks."""
    present = dict(zip(_normalised(columns), columns, strict=True))
    return ", ".join(
        f'"{present[c]}" AS {c}'
        if c in present
        else f"CAST(NULL AS VARCHAR) AS {c}"
        for c in superset
    )


def clean_archive(
    con: duckdb.DuckDBPyConnection,
    archive: Path,
    dest: Path,
    superset: list[str],
) -> int:
    tmp = RS_INPUT / f".{archive.stem}.csv"
    for out_sep in OUT_SEP_CANDIDATES:
        try:
            with zipfile.ZipFile(archive) as z:
                recovered, repaired = _transcode(
                    z, _sole_csv(z, archive), tmp, out_sep
                )
            break
        except DelimiterCollisionError:
            print(
                f"    {archive.name}: contains {out_sep!r}, trying the next "
                f"delimiter",
                flush=True,
            )
    else:
        tmp.unlink(missing_ok=True)
        raise SystemExit(
            f"{archive.name}: every candidate delimiter occurs in the source"
        )
    notes = []
    if recovered:
        notes.append(f"{recovered} byte(s) undefined in cp1252 -> latin-1")
    if repaired:
        notes.append(f"{repaired} ragged row(s) rejoined into the last column")

    try:
        # The temp file uses SOH as its delimiter (see OUT_SEP) and no quoting, so
        # `"` and `;` are both ordinary data by construction.
        rel = (
            f"read_csv('{tmp}', delim='{out_sep}', header=true, all_varchar=true, "
            f"encoding='utf-8', quote='', ignore_errors=false)"
        )
        out_path = dest / f"data_{_partition_of(archive)}.parquet"
        con.execute(
            f"COPY (SELECT {_projection(_header_of(archive), superset)} FROM {rel}) "
            f"TO '{out_path}' (FORMAT PARQUET, COMPRESSION SNAPPY)"
        )
        n = con.execute(
            f"SELECT count(*) FROM read_parquet('{out_path}')"
        ).fetchone()[0]
    finally:
        # Always: a 660 MB temp left behind on failure fills the disk within a dozen
        # archives.
        tmp.unlink(missing_ok=True)

    if n == 0:
        # An empty first partition makes dump_header infer INTEGER for every column and
        # poisons the staging schema. See
        # reference_empty_parquet_partition_poisons_staging_schema.
        out_path.unlink()
        print(f"    {archive.name}: 0 rows, parquet dropped")
        return 0
    suffix = f"   [{'; '.join(notes)}]" if notes else ""
    print(f"    {archive.name}: {n:,} rows{suffix}", flush=True)
    return n


def main(only_year: int | None = None) -> None:
    archives = sorted(RS_INPUT.glob("*.zip"))
    if not archives:
        raise SystemExit(
            f"no archives under {RS_INPUT}; run download_rs.py first"
        )

    # Built from EVERY archive, not just the ones being converted, so a single-year
    # rerun still writes the full column set and stays union-compatible with the
    # partitions already on disk.
    superset = superset_columns(archives)
    print(
        f"superset schema: {len(superset)} columns across {len(archives)} archives"
    )

    if only_year is not None:
        archives = [a for a in archives if a.name.startswith(str(only_year))]
        if not archives:
            raise SystemExit(f"no archives for {only_year}")

    dest = OUTPUT_DIR / RS_TABLE
    dest.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    con.execute("SET preserve_insertion_order=false")
    con.execute("PRAGMA memory_limit='2GB'")
    con.execute("PRAGMA threads=2")

    # Resumable. Each archive is independent, and the whole series is a ~40 minute
    # job that has been killed mid-run by memory pressure more than once; restarting
    # from zero each time is how a long conversion never finishes. A partition already
    # written against the CURRENT superset is trusted, and anything else is rebuilt.
    # Six months are published TWICE under the neighbouring month's file name, as a
    # partial and a complete snapshot of the same month (2020-07 appears as 121,940 and
    # 205,468 rows). Two archives therefore target one partition, and the previous code
    # kept whichever sorted last -- which happened to be the complete one in all six
    # cases, by luck rather than design. Choose the larger member explicitly, so a
    # reordering upstream cannot silently swap in the partial snapshot.
    by_partition: dict[str, list[Path]] = {}
    for archive in archives:
        by_partition.setdefault(_partition_of(archive), []).append(archive)
    chosen: list[Path] = []
    for name, group in sorted(by_partition.items()):
        if len(group) > 1:
            group = sorted(group, key=_member_size, reverse=True)
            print(
                f"    data_{name}: {len(group)} archives claim this month; keeping "
                f"{group[0].name} ({_member_size(group[0]) / 1e6:.0f} MB) over "
                + ", ".join(
                    f"{a.name} ({_member_size(a) / 1e6:.0f} MB)"
                    for a in group[1:]
                )
            )
        chosen.append(group[0])
    archives = sorted(chosen)

    total = 0
    for archive in archives:
        out_path = dest / f"data_{_partition_of(archive)}.parquet"
        if out_path.exists():
            existing = con.execute(
                f"SELECT count(*) FROM (DESCRIBE SELECT * FROM "
                f"read_parquet('{out_path}'))"
            ).fetchone()[0]
            if existing == len(superset):
                n = con.execute(
                    f"SELECT count(*) FROM read_parquet('{out_path}')"
                ).fetchone()[0]
                total += n
                print(
                    f"    {archive.name}: {n:,} rows (already built)",
                    flush=True,
                )
                continue
            out_path.unlink()
        total += clean_archive(con, archive, dest, superset)

    files = sorted(dest.glob("data_*.parquet"))
    if not files:
        print(f"  {RS_TABLE}: no non-empty partitions")
        return

    # Every partition must agree on its columns, or the BigQuery load drops the
    # difference in silence. `union_by_name=false` makes a disagreement raise here
    # rather than being papered over locally and failing later.
    widths = con.execute(
        f"SELECT count(*) FROM (DESCRIBE SELECT * FROM "
        f"read_parquet('{dest}/data_*.parquet', union_by_name=false))"
    ).fetchone()[0]
    if widths != len(superset):
        raise SystemExit(
            f"{RS_TABLE}: partitions disagree -- {widths} columns read against a "
            f"{len(superset)}-column superset"
        )

    typed = con.execute(
        f"SELECT DISTINCT column_name, column_type FROM (DESCRIBE SELECT * FROM "
        f"read_parquet('{dest}/data_*.parquet')) WHERE column_type <> 'VARCHAR'"
    ).fetchall()
    if typed:
        raise SystemExit(
            f"{RS_TABLE}: staging must be all-STRING, found {typed}"
        )

    # The calendar gap that no file count can see. RS publishes six months under the
    # NEIGHBOURING month's file name, so the series arrives with 175 archives, every
    # CRC valid, twelve files per exercise -- and six months absent: 2020-06, 2020-08,
    # 2022-04, 2023-02, 2023-06 and 2023-08. Only grouping by the data's own
    # (Exercicio, Mes) reveals it, so that check lives here rather than in a
    # to-do somewhere.
    present = {
        (int(a), int(m))
        for a, m in con.execute(
            f"SELECT DISTINCT ano, mes FROM read_parquet('{dest}/data_*.parquet') "
            f"WHERE ano IS NOT NULL AND mes IS NOT NULL"
        ).fetchall()
    }
    if present:
        lo, hi = min(present), max(present)
        expected = {
            (y, m)
            for y in range(lo[0], hi[0] + 1)
            for m in range(1, 13)
            if (y, m) >= lo and (y, m) <= hi
        }
        gaps = sorted(expected - present)
        if gaps:
            print(
                f"  WARNING {len(gaps)} month(s) absent from the source between "
                f"{lo[0]}-{lo[1]:02d} and {hi[0]}-{hi[1]:02d}: "
                + ", ".join(f"{y}-{m:02d}" for y, m in gaps)
            )

    span = con.execute(
        f"SELECT min(ano), max(ano), count(DISTINCT ano), count(DISTINCT fasegasto) "
        f"FROM read_parquet('{dest}/data_*.parquet')"
    ).fetchone()
    print(
        f"  {RS_TABLE}: {total:,} rows across {len(files)} files, {widths} columns "
        f"-> ano {span[0]}-{span[1]} ({span[2]} distinct), {span[3]} phases"
    )


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--year", type=int, help="convert a single exercise")
    args = ap.parse_args()
    main(only_year=args.year)
