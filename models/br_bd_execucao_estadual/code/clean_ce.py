"""Convert Ceará source files to all-STRING staging parquet.

Three staging tables come out, one per phase: `ce_empenho`, `ce_liquidacao`,
`ce_pagamento`. CE is the second-longest series in this dataset (empenho from 2006) and
the third complete empenho -> liquidação -> pagamento chain, after SC and PB.

What makes CE hard is not access -- see `download_ce.py` for that -- but that twenty
years of exports were produced by several generations of tooling that agree on almost
nothing:

* **Five container formats.** `.rar` (2006-2017), `.csv`, `.csv.zip`/`.zip`, `.xlsx`
  (2019-2024), `.xls` (2024+). `.rar` is read through `bsdtar`, because `rarfile`'s
  pure-Python fallback truncates the member (`Failed the read enough data: req=1500
  got=41`) while listing it happily, and no `unrar` is installed. The catalogue's one
  `.ods` belongs to dataset 145, which is excluded as a duplicate of dataset 152 -- see
  `constants.CE_EMPENHO_DUPLICATE_SLOT` -- so no ODF reader is needed.
  A container's extension is a hint, not a fact: `read_rows` dispatches on magic bytes.

* **Eleven schemas across three phases** -- six for empenho, three for pagamento, two
  for liquidação -- with disjoint column names and, more importantly, different content.
  The early eras publish organisational, creditor and budget **codes**; the modern eras
  publish **labels** and drop the codes entirely. Folding them onto shared names would
  put codes and labels in one field, so each era keeps its own names and every partition
  is written to the **superset** of all of them. That is the RS rule, and it is required
  here: `upload.py` loads a table's parquet with one wildcard, and a wildcard load infers
  ONE schema and silently drops columns the other files have.

* **One file with its own dialect.** `notas_de_empenho_2026.csv` is semicolon-separated
  and cp1252 where the other 215 are comma-separated and UTF-8. Both are probed per file.

* **Rows that are the right width and the wrong shape.** Padding, split free-text fields
  and stray empty commas interact so that a broken row can arrive at exactly the expected
  field count with every column holding its neighbour's value. `_place` is where that is
  dealt with, and the reasoning there is the most load-bearing part of this file.

* **A null spelled out.** 36,760,936 fields across 104 files hold the literal string
  `NULL`. They are written as real nulls -- the only value this cleaner changes.

Every file's header is asserted against the registry in `constants.CE_SCHEMAS`. An
unrecognised header is a hard failure with the diff printed, never a silent
`union_by_name`: that is the PE defect, which left 1,031,326 rows present and entirely
NULL.

Rows are partitioned by **the exercise the data itself carries**, never by the dataset
the file was filed under. Dataset 135 is titled "Notas de Liquidação de Despesa - 2020"
and contains two files named `NLD_Ano2019_*`.

Staging is all-STRING, by house convention and for a concrete reason: the recurring
pipeline's upload path stringifies its header, so a typed external table left behind by
onboarding collides with the later all-STRING overwrite. See
.claude/rules/prefect-pipeline-conventions.md, "Staging parquet must be all-STRING".
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import io
import json
import re
import subprocess
import sys
import zipfile
from collections import Counter, defaultdict
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent))
from constants import (
    CE_EMPENHO_DUPLICATE_SLOT,
    CE_ENCODING,
    CE_EXERCICIO_COLUMNS,
    CE_FALLBACK_ENCODING,
    CE_FREE_TEXT_COLUMNS,
    CE_HEADER_FIXES,
    CE_MAX_SPLIT,
    CE_NULL_SENTINEL,
    CE_SCHEMAS,
    CE_SEP,
    CE_SEP_CANDIDATES,
    CE_TABLES,
    INPUT_DIR,
    OUTPUT_DIR,
    normalise_column,
)

CE_INPUT = INPUT_DIR / "ce"
CE_PHASES = tuple(CE_TABLES)

# csv's default 128 KB field cap is not enough: `especificacaogeral` carries whole
# contract descriptions.
csv.field_size_limit(1 << 24)


class SchemaUnknownError(Exception):
    """A file's header matches no registered era."""


# --------------------------------------------------------------------- readers


def _sniff_sep(text: str) -> str:
    """The delimiter this file uses, decided from its header line.

    **215 of CE's 216 files are comma-separated; `notas_de_empenho_2026.csv` is
    semicolon-separated and fully quoted.** One file in a 216-file series uses a
    different dialect, so the delimiter is measured per file rather than assumed. Parsed
    with the wrong one, that file yields a single 1-column "header" -- which the schema
    registry would reject, but with a message blaming the schema rather than the parse.

    Decided on the header alone: a data line's free text is full of commas either way,
    while the header's separators are the only ones present in it.
    """
    line = text.split("\n", 1)[0]
    counts = {sep: line.count(sep) for sep in CE_SEP_CANDIDATES}
    best = max(counts, key=lambda s: counts[s])
    if counts[best] == 0:
        raise SystemExit(
            f"no candidate delimiter occurs in the header line: {line[:120]!r}"
        )
    return best


def _csv_rows(text: str, sep: str) -> list[list[str]]:
    """Quote-aware parse.

    **In the comma dialect the field separator and the decimal separator are the same
    character.** Values carrying a decimal are quoted (`"14145,17"`) and integers are
    not, so the quoting is what keeps the fields aligned; splitting on `,` shifts every
    row that has a non-integer value. Free text carries embedded commas, quotes and real
    newlines, all of which a quote-aware reader places correctly -- where the source
    quoted it, which it does not always do. See `_repair`.
    """
    return list(csv.reader(io.StringIO(text, newline=""), delimiter=sep))


def _decode(raw: bytes) -> str:
    """Decode one file, probing rather than assuming.

    CE is UTF-8 in 215 of its 216 files. The exception is `notas_de_empenho_2026.csv`,
    which is **cp1252** -- `"Exerc\\xedcio"` in the first eight bytes.

    **"utf-8 strict, else cp1252" is a legitimate probe; "utf-8, else latin-1" is not.**
    UTF-8 is self-validating and cp1252 leaves five bytes undefined, so the pair
    discriminates. latin-1 decodes any byte sequence, can therefore never fail, and
    would ship mojibake reporting success -- the RS lesson.
    """
    try:
        return raw.decode(CE_ENCODING)
    except UnicodeDecodeError:
        return raw.decode(CE_FALLBACK_ENCODING)


def _text_rows(raw: bytes) -> tuple[list[list[str]], str]:
    text = _decode(raw)
    sep = _sniff_sep(text)
    return _csv_rows(text, sep), sep


def _rar_bytes(path: Path) -> bytes:
    """Extract a single-member `.rar` through bsdtar.

    `rarfile` lists the member but cannot read it without an external `unrar`/`unar`,
    and its fallback fails mid-stream (`BadRarFile: Failed the read enough data`) rather
    than refusing up front -- a truncated read that looks like a short file. bsdtar
    (libarchive) is present on macOS and reads these archives whole.
    """
    proc = subprocess.run(
        ["bsdtar", "-xOf", str(path)], capture_output=True, check=False
    )
    if proc.returncode != 0 or not proc.stdout:
        raise SystemExit(
            f"{path.name}: bsdtar could not extract the archive "
            f"({proc.returncode}, {proc.stderr[:200]!r})"
        )
    return proc.stdout


def _zip_bytes(path: Path) -> bytes:
    with zipfile.ZipFile(path) as zf:
        names = [n for n in zf.namelist() if not n.endswith("/")]
        if len(names) != 1:
            raise SystemExit(
                f"{path.name}: expected one member, found {names}"
            )
        return zf.read(names[0])


def _cell(value) -> str:
    """One spreadsheet cell as the string the CSV era would have written.

    `str()` on a spreadsheet cell is the trap `astype(str)` is elsewhere: an integer
    read back as a float renders `2018.0`, and a NULL renders `nan`. Integral floats are
    emitted without the fractional part and empties as the empty string.
    """
    if value is None:
        return ""
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return str(int(value)) if value.is_integer() else repr(value)
    if isinstance(value, (dt.datetime, dt.date)):
        return value.strftime("%d/%m/%Y")
    return str(value)


def _xlsx_rows(path: Path) -> list[list[str]]:
    import openpyxl

    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    if len(wb.sheetnames) != 1:
        print(
            f"    {path.name}: sheets {wb.sheetnames}, reading the first",
            flush=True,
        )
    ws = wb[wb.sheetnames[0]]
    rows = [[_cell(c) for c in r] for r in ws.iter_rows(values_only=True)]
    wb.close()
    return rows


def _xls_rows(path: Path) -> list[list[str]]:
    import xlrd

    wb = xlrd.open_workbook(str(path))
    sheet = wb.sheet_by_index(0)
    out: list[list[str]] = []
    for i in range(sheet.nrows):
        row: list[str] = []
        for value, ctype in zip(
            sheet.row_values(i), sheet.row_types(i), strict=True
        ):
            if ctype == xlrd.XL_CELL_DATE:
                row.append(
                    xlrd.xldate_as_datetime(value, wb.datemode).strftime(
                        "%d/%m/%Y"
                    )
                )
            else:
                row.append(_cell(value))
        out.append(row)
    return out


def read_rows(path: Path) -> tuple[list[list[str]], str]:
    """Rows of one source file, whatever it is wrapped in, all cells as strings.

    **CE mislabels containers.** A file served as `.xls` may be a real BIFF workbook, a
    zip-based `.xlsx`, or plain CSV text. The extension is therefore a hint and the
    magic bytes decide, which is why this dispatches on content for the spreadsheet
    formats instead of trusting the name.
    """
    low = path.name.lower()
    if low.endswith(".rar"):
        return _text_rows(_rar_bytes(path))
    if low.endswith(".zip"):
        return _text_rows(_zip_bytes(path))
    if low.endswith(".csv"):
        return _text_rows(path.read_bytes())

    head = path.open("rb").read(8)
    if head[:2] == b"PK":  # xlsx/ods are zip containers
        return _xlsx_rows(path), CE_SEP
    if head[:4] == b"\xd0\xcf\x11\xe0":  # OLE2 = real .xls
        return _xls_rows(path), CE_SEP
    if low.endswith((".xls", ".xlsx")):
        # Served with a spreadsheet extension but the bytes are text.
        print(
            f"    {path.name}: spreadsheet extension, CSV content", flush=True
        )
        return _text_rows(path.read_bytes())
    raise SystemExit(f"{path.name}: unrecognised container ({head[:8]!r})")


# --------------------------------------------------------------------- shaping


_DATE_RE = re.compile(
    r"^(\d{4}-\d{2}-\d{2}|\d{2}/\d{2}/\d{4})([ T]\d{2}:\d{2}.*)?$"
)
_TIME_RE = re.compile(r"^\d{1,2}:\d{2}(:\d{2})?$")
_NUM_RE = re.compile(r"^-?\d{1,3}(\.\d{3})*(,\d+)?$|^-?\d+([.,]\d+)?$")


def _anchor_kind(name: str) -> str | None:
    """The type a column name promises, for validating a repaired row.

    Only columns whose contents are constrained are used. A free-text or code column
    validates against anything and so proves nothing about where the fields landed.
    """
    if name in (
        "exercicio",
        "num_ano",
        "num_ano_emp",
        "exercicio_restos_a_pagar",
    ):
        return "year"
    if name.startswith("hr_"):
        return "time"
    # `dth_` is the 2006-2016 spelling and `data`/`dt_` the later ones. Missing `dth_`
    # left the two oldest eras -- 1.86M empenho and 2.77M pagamento rows -- with almost
    # no constrained columns, so a shifted row there had nothing to fail against.
    if name.startswith(("data", "dt_", "dth_")):
        return "date"
    if name.startswith(("valor", "vlr_")):
        return "number"
    return None


def _validates(value: str, kind: str) -> bool:
    v = value.strip()
    # The 2018+ exports spell a null as the literal string NULL -- 36.7M of them. For
    # placing a row it is an empty field, and it is written out as a real null.
    if v == "" or v == CE_NULL_SENTINEL:
        return True
    if kind == "year":
        return len(v) == 4 and v.isdigit()
    if kind == "time":
        return bool(_TIME_RE.match(v))
    if kind == "date":
        return bool(_DATE_RE.match(v))
    if kind == "number":
        return bool(_NUM_RE.match(v))
    return True


def _anchors_ok(row: list[str], anchors: list[tuple[int, str]]) -> bool:
    return all(_validates(row[i], kind) for i, kind in anchors)


def _empty_runs(row: list[str]) -> list[tuple[int, int]]:
    runs: list[tuple[int, int]] = []
    start = None
    for i, value in enumerate(row):
        if value.strip() == "":
            start = i if start is None else start
        elif start is not None:
            runs.append((start, i - start))
            start = None
    if start is not None:
        runs.append((start, len(row) - start))
    return runs


def _named_width(header: list[str]) -> int:
    """Columns the header actually names.

    The legacy `.rar` exports append a varying number of bare commas to the header and
    to every data row: measured 1, 2, 10 and 25 across the four quarters of 2017
    empenho alone, and 56 on one pagamento quarter. A header signature taken at face
    value reports six distinct schemas for a series that has one.
    """
    width = len(header)
    while width and header[width - 1].strip() == "":
        width -= 1
    return width


def _header_of(raw_header: list[str]) -> list[str]:
    """Column names folded to the house convention, with known breakages repaired.

    `normalise_column` handles what BigQuery rejects. `CE_HEADER_FIXES` handles one
    column whose NAME is a failed i18n lookup rather than a name -- see that constant.
    """
    names = [
        normalise_column(c) for c in raw_header[: _named_width(raw_header)]
    ]
    return [CE_HEADER_FIXES.get(n, n) for n in names]


def _free_text_indices(header: list[str]) -> list[int]:
    """Every column in this schema the source is known to under-quote.

    There is more than one per schema and the split is not always in the obvious one.
    The legacy empenho export splits `especificacaogeral` in most cases but
    `razaosocialcredor` in others (`...,3301349000151,F,8825,2200010012015C,...`), so
    trying only the first match leaves rows unplaceable.
    """
    return sorted(header.index(n) for n in CE_FREE_TEXT_COLUMNS if n in header)


def _rejoin(
    row: list[str], free_text: int, absorb: int, sep: str
) -> list[str]:
    """Merge `absorb + 1` consecutive fields back into the free-text column."""
    return [
        *row[:free_text],
        sep.join(row[free_text : free_text + absorb + 1]),
        *row[free_text + absorb + 1 :],
    ]


def _fit(
    row: list[str], width: int, allow_pad: bool = False
) -> list[str] | None:
    """Trim trailing empties -- and, when asked, pad with them -- to reach `width`.

    **Padding is off by default, and that is what makes the rejoin unambiguous.** With
    padding allowed, merging too many fields also lands on the right width: the row comes
    up short and gets topped up with empties, so absorbing one field and absorbing three
    both "fit" and the type check cannot separate them. Ten such candidates survived on
    the 2018 zip before this was split in two. A rejoin must therefore reach the width by
    consuming the export's real trailing padding, never by inventing any.

    Padding stays available to the empty-field deletion, which genuinely needs it: that
    repair removes a field from the middle of a row already at full width, and the value
    that falls off the end is one of those same trailing empties.
    """
    out = list(row)
    while len(out) > width and out[-1].strip() == "":
        out.pop()
    if allow_pad and len(out) < width:
        out += [""] * (width - len(out))
    return out if len(out) == width else None


def _place(
    row: list[str],
    anchors: list[tuple[int, str]],
    width: int,
    free_text: list[int],
    sep: str,
) -> tuple[list[str], str]:
    """Put one row into the columns the header names, or refuse to guess.

    **Three defects in this source misplace a row, all measured, and the width tells
    them apart only sometimes.**

    1. *Unnamed trailing padding.* The legacy exports append a varying number of bare
       commas to the header and every row -- 0, 1, 2, 10, 25 and 56 across files of one
       series. Harmless alone; it is what makes the other two hard, because it absorbs
       an extra field and leaves a broken row at exactly the expected width.

    2. *A split free-text field.* `especificacaogeral`, `justificativa` and the creditor
       name are quoted inconsistently, so a value containing the separator sometimes
       splits the record:
           `pagamento de diária e ajuda de custo mês de abril, portaria 585/2018`
           `F, TARCISIO G. PARENTE - ME`
           `PAGAMENTO OBRA DE ENGENHARIA 2015, SEM RETENÇÃO DE ISS ...`

    3. *A spurious empty field* inside a run of empty columns, which shifts every later
       value one column right.

    **Defects 2 and 3 are the same shape and need opposite repairs, so they are told
    apart by what is being merged, not by where.** A rejoin is only a real rejoin if the
    fields it merges contain text; merging two empty fields would silently invent a
    `justificativa` of `","` for the 342 rows of `npd-2017-terceiro-trimestre` whose
    actual defect is a stray comma. So a rejoin is attempted only over non-empty pieces,
    and the empty-field deletion is what handles the rest.

    Every candidate is then checked against the schema's typed columns -- exercise,
    dates, times, money -- and accepted only if exactly one distinct row survives.
    That check is the whole safeguard: a rejoin at the wrong column, or a deletion from
    the wrong run, still produces a row of the correct WIDTH. Width proves nothing here,
    which is exactly why relaxing the parser corrupts this source rather than failing on
    it.
    """
    candidates: dict[tuple[str, ...], str] = {}

    exact = _fit(row, width)
    if exact is not None and _anchors_ok(exact, anchors):
        return exact, "clean"

    # Smallest repair first. A larger absorb consumes less of the export's trailing
    # padding and more of the row's real data, so where several absorb widths reach the
    # right width the smallest is the one that merged only what the separator split.
    for absorb in range(1, CE_MAX_SPLIT + 1):
        for column in free_text:
            pieces = row[column : column + absorb + 1]
            if len(pieces) < absorb + 1:
                continue
            if not any(p.strip() for p in pieces):
                continue  # merging empties is not a rejoin -- see the docstring
            fitted = _fit(_rejoin(row, column, absorb, sep), width)
            if fitted is not None and _anchors_ok(fitted, anchors):
                return fitted, "rejoined"

    for start, length in _empty_runs(row):
        for drop in range(1, length + 1):
            fitted = _fit(
                row[:start] + row[start + drop :], width, allow_pad=True
            )
            if fitted is not None and _anchors_ok(fitted, anchors):
                candidates.setdefault(tuple(fitted), "unshifted")

    if len(candidates) == 1:
        values, how = next(iter(candidates.items()))
        return list(values), how

    failing = [
        (i, row[i], kind)
        for i, kind in anchors
        if i < len(row) and not _validates(row[i], kind)
    ]
    raise SystemExit(
        f"row of {len(row)} fields (expected {width}) could not be placed: "
        f"{len(candidates)} distinct repair(s) survive the type check, failing "
        f"columns {failing[:3]}. Do NOT relax the check -- measure what changed. "
        f"Row starts {row[:6]}"
    )


def _normalise_widths(
    rows: list[list[str]], header: list[str], sep: str = CE_SEP
) -> tuple[list[list[str]], Counter]:
    """Bring every row into the header's columns, counting how each was placed."""
    width = _named_width(header)
    anchors = [
        (i, kind)
        for i, name in enumerate(header[:width])
        if (kind := _anchor_kind(name)) is not None
    ]
    free_text = _free_text_indices(header)
    out: list[list[str]] = []
    how = Counter()
    for raw_row in rows:
        row = list(raw_row)
        if len(row) < width and not any(c.strip() for c in row):
            continue
        if len(row) < width:
            raise SystemExit(
                f"row has {len(row)} fields, fewer than the {width} the header names, "
                f"and is not blank: {row[:8]}"
            )
        placed, kind = _place(row, anchors, width, free_text, sep)
        how[kind] += 1
        out.append(placed)
    return out, how


def _match_era(phase: str, header: list[str]) -> str:
    for era, columns in CE_SCHEMAS[phase].items():
        if header == columns:
            return era
    known = {e: c for e, c in CE_SCHEMAS[phase].items()}
    best, score = None, -1
    for era, columns in known.items():
        overlap = len(set(header) & set(columns))
        if overlap > score:
            best, score = era, overlap
    missing = [c for c in known[best] if c not in header]
    extra = [c for c in header if c not in known[best]]
    raise SchemaUnknownError(
        f"{phase}: header of {len(header)} column(s) matches no registered era. "
        f"Closest is {best!r} ({score} shared); missing={missing}; extra={extra}. "
        f"Register the era deliberately -- unioning it silently would leave one "
        f"schema's columns NULL for every row of the other."
    )


def _superset(phase: str) -> list[str]:
    """Every column of every era, in era order, each appearing once.

    All partitions of a table are written to this schema. `upload.py` loads a table's
    parquet directory with a single wildcard, and **a wildcard parquet load infers one
    schema and silently drops columns the other files have**, while still reporting the
    full row count.
    """
    out: list[str] = []
    for columns in CE_SCHEMAS[phase].values():
        for c in columns:
            if c not in out:
                out.append(c)
    return out


def _exercicio_index(phase: str, header: list[str]) -> int:
    for candidate in CE_EXERCICIO_COLUMNS:
        if candidate in header:
            return header.index(candidate)
    raise SystemExit(
        f"{phase}: no exercise column in {header[:6]}... -- partitioning by the "
        f"dataset's slot year instead is exactly the defect this guards against."
    )


# --------------------------------------------------------------------- writing


def clean_file(
    source: Path, phase: str, out_dir: Path, superset: list[str]
) -> dict[str, int]:
    """One source file -> one parquet per exercise it actually contains."""
    rows, sep = read_rows(source)
    if not rows:
        print(f"    {source.name}: empty file", flush=True)
        return {}
    header = _header_of(rows[0])
    era = _match_era(phase, header)
    try:
        body, how = _normalise_widths(rows[1:], header, sep)
    except SystemExit as exc:
        raise SystemExit(f"{source.name}: {exc}") from None
    defects = {k: v for k, v in how.items() if k != "clean"}
    if defects:
        print(f"    {source.name}: repaired {dict(defects)}", flush=True)

    ycol = _exercicio_index(phase, header)
    by_year: dict[str, list[list[str]]] = defaultdict(list)
    for r in body:
        by_year[r[ycol].strip()].append(r)

    index = {name: i for i, name in enumerate(header)}
    out_dir.mkdir(parents=True, exist_ok=True)
    written: dict[str, int] = {}

    for year, group in sorted(by_year.items()):
        if not group:
            continue
        # Columns the era does not publish are written as nulls, not as "", so a
        # missing column is distinguishable downstream from a present-but-blank one.
        arrays = [
            pa.array(
                [
                    None
                    if name not in index
                    or (value := g[index[name]]) == CE_NULL_SENTINEL
                    else value
                    for g in group
                ],
                type=pa.string(),
            )
            for name in superset
        ]
        table = pa.Table.from_arrays(arrays, names=superset)
        stem = source.name.rsplit(".", 1)[0]
        dest = out_dir / f"data_{year or 'sem_exercicio'}__{stem}.parquet"
        pq.write_table(table, dest, compression="snappy")
        written[year] = len(group)

    total = sum(written.values())
    print(
        f"    {source.name[:60]:<60} {era:<8} {total:>9,} rows "
        f"{sorted(written)}",
        flush=True,
    )
    return written


def sources_for(phase: str, years: set[int] | None) -> list[Path]:
    paths: list[Path] = []
    for meta in sorted((CE_INPUT / phase).glob("*.meta.json")):
        info = json.loads(meta.read_text())
        src = Path(str(meta)[: -len(".meta.json")])
        if not src.exists():
            continue
        # Dataset 145 republishes 2023 empenho as four quarterly files that dataset 152
        # already carries as one consolidated CSV. Staging both double-counts 2023.
        if int(info["dataset_id"]) == CE_EMPENHO_DUPLICATE_SLOT:
            continue
        if years and info["slot_year"] not in years:
            continue
        paths.append(src)
    return paths


def main(
    phases: tuple[str, ...] = CE_PHASES, years: set[int] | None = None
) -> None:
    grand: dict[str, Counter] = {}
    for phase in phases:
        table = CE_TABLES[phase]
        superset = _superset(phase)
        paths = sources_for(phase, years)
        if not paths:
            print(f"  {phase}: no input files found", flush=True)
            continue
        print(
            f"  {phase} -> {table} ({len(paths)} file(s), "
            f"{len(superset)} superset columns)",
            flush=True,
        )
        counts: Counter = Counter()
        for src in paths:
            for year, n in clean_file(
                src, phase, OUTPUT_DIR / table, superset
            ).items():
                counts[year] += n
        grand[table] = counts
        print(f"  == {table}: {sum(counts.values()):,} rows", flush=True)
        for year in sorted(counts):
            print(
                f"       {year or '(blank)'}: {counts[year]:>10,}", flush=True
            )

    print()
    for table, counts in grand.items():
        print(
            f"{table:<16} {sum(counts.values()):>12,} rows, {len(counts)} exercise(s)"
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase", choices=CE_PHASES, action="append")
    parser.add_argument("--year", type=int, action="append")
    args = parser.parse_args()
    main(
        phases=tuple(args.phase) if args.phase else CE_PHASES,
        years=set(args.year) if args.year else None,
    )
