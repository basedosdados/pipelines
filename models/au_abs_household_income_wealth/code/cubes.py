"""Structural parser for the ABS Household Income and Wealth (6523.0) data cubes.

The cubes are presentation spreadsheets, not the ASNA time-series layout that
``models/au_abs_national_accounts/code/clean_data.py`` handles, so that parser
does not apply. The long shape it produces does: this module reduces every cube
sheet to one record per published cell.

Every sheet shares one grammar:

  * three rows of ABS boilerplate, then a ``Table N.M  TITLE`` row. A sheet
    repeats that boilerplate to open each *block*, and a block re-declares its
    own column headers, so blocks are parsed independently.
  * one to three column-header rows, recognised by an empty column A. Spanning
    headers live in merged cells and are filled down from their anchor.
  * data rows, whose column-A label nests either by cell indent or by three
    leading spaces per level, depending on the sheet.
  * footnote rows, which carry no data.

Alongside each estimate ABS publishes its relative standard error or, for a
proportion, its 95% margin of error. Which of the three a cell holds is marked
in one of three different places, and a single cube mixes them:

  1. a unit in column B — ``$`` against ``RSE(%)`` against ``MOE(±)``;
  2. a header row spanning a group of columns (table 2.3);
  3. an ALL-CAPS banner alone in column A, opening a row section (table 1.1).

``cell_kind`` reads all three, in that order of authority. An estimate is then
joined to its own error measure on (row label path, physical column), which is
stable because the panels mirror each other's layout. Where one sheet holds two
estimate panels with the same row labels — table 5.4 states mean weekly gross
income and mean weekly equivalised income in turn — that key collides, and the
section ordinal is added to it: the n-th estimate section is described by the
n-th RSE section.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

import openpyxl

ABS_BOILERPLATE = "australian bureau of statistics"
MAX_COL = 30
MAX_ROW = 2000

# Footnote markers: "(a)", "(b)(c)". Never "(P10)", "(%)" or "($'000)".
FOOTNOTE_MARKER = re.compile(r"\(([a-z])\)")
# A unit at the end of a banner: "WEEKLY VALUE ($)", "ESTIMATES ($'000)".
BANNER_UNIT = re.compile(r"\s*\(([^)]*)\)\s*$")

RSE_PREFIX = "RELATIVE STANDARD ERROR"
MOE_PREFIX = "95% MARGIN OF ERROR"

# Banners that name the panel or restate the unit rather than saying what is
# measured. Everything else is kept, so a sheet holding two estimate panels
# keeps them apart.
PANEL_ONLY_BANNERS = {"ESTIMATES", "WEEKLY VALUE"}

# Units, wherever they appear: column B, a header row, or a banner bracket.
UNIT_TOKENS = {
    "$",
    "%",
    "no.",
    "'000",
    "$'000",
    "$ '000",
    "ratio",
    "years",
    "hours",
    "±",
    "rse(%)",
    "moe(±)",
}
RSE_UNITS = {"rse(%)"}
MOE_UNITS = {"moe(±)"}

# Markers ABS publishes in place of a value. Kept verbatim: casting them to
# zero would turn "not published" into a published estimate of nil.
VALUE_FLAGS = {
    "np": "np",
    "n.p.": "np",
    "na": "na",
    "n.a.": "na",
    "..": "..",
    "-": "-",
    "—": "-",
    "–": "-",
    "*": "*",
    "**": "**",
}


def norm(value) -> str:
    """Collapse whitespace; ``None`` becomes the empty string."""
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()


def strip_footnotes(label: str) -> str:
    """Drop ``(a)``-style markers so mirrored panels carry the same labels."""
    return re.sub(r"\s+", " ", FOOTNOTE_MARKER.sub("", label)).strip()


def is_unit(text: str) -> bool:
    return text.strip().lower() in UNIT_TOKENS


def is_banner(label: str, rest: list[str]) -> bool:
    """An ALL-CAPS label alone on its row opens a section."""
    if not label or any(value != "" for value in rest) or len(label) > 150:
        return False
    return len(re.findall(r"[A-Za-z]", label)) >= 3 and label.upper() == label


def kind_of(text: str) -> str:
    """Which quantity a banner, header label or unit announces."""
    stripped = text.strip()
    if stripped.lower() in RSE_UNITS or stripped.startswith(RSE_PREFIX):
        return "rse"
    if stripped.lower() in MOE_UNITS or stripped.startswith(MOE_PREFIX):
        return "moe"
    return "estimate"


def split_banner(banner: str) -> tuple[str, str, str]:
    """Return a banner's kind, the unit it carries, and the measure it names."""
    kind = kind_of(banner)
    unit, stem = "", banner
    match = BANNER_UNIT.search(stem)
    if match and (is_unit(match.group(1)) or not match.group(1)):
        unit = match.group(1).strip()
        stem = stem[: match.start()].strip()
    if kind != "estimate":
        # A "(%)" or "(±)" there sizes the error, not the estimate.
        return kind, "", ""
    return kind, unit, "" if stem in PANEL_ONLY_BANNERS else stem


def parse_number(text: str) -> tuple[float | None, str]:
    """Split a data cell into a number and, failing that, an ABS marker."""
    flag = VALUE_FLAGS.get(text.strip().lower())
    if flag:
        return None, flag
    try:
        return float(text.replace(",", "")), ""
    except ValueError:
        return None, ""


@dataclass
class Record:
    """One published cell, before estimates and their error measures are joined."""

    kind: str
    section: int
    row_path: tuple[str, ...]
    column: int
    col_path: tuple[str, ...]
    unit: str
    measure_prefix: str
    value: float | None
    flag: str


@dataclass
class SheetParse:
    table_id: str
    table_name: str
    records: list[Record] = field(default_factory=list)
    banners: list[str] = field(default_factory=list)
    dropped_numeric: int = 0


def _grids(worksheet):
    """Raw cell values, plus a copy with merged ranges filled from the anchor.

    Only header rows may read the filled grid: filling the data region would
    smear a banner across every column of its row.
    """
    n_rows = min(worksheet.max_row or 1, MAX_ROW)
    raw = [
        [norm(cell.value) for cell in row]
        for row in worksheet.iter_rows(
            min_row=1, max_row=n_rows, max_col=MAX_COL
        )
    ]
    filled = [list(row) for row in raw]
    for rng in worksheet.merged_cells.ranges:
        r0, c0 = rng.min_row - 1, rng.min_col - 1
        if r0 >= len(filled) or c0 >= MAX_COL:
            continue
        anchor = filled[r0][c0]
        if not anchor:
            continue
        for r in range(r0, min(rng.max_row, len(filled))):
            for c in range(c0, min(rng.max_col, MAX_COL)):
                filled[r][c] = anchor
    return raw, filled


def _depths(worksheet, n_rows: int) -> list[int]:
    """Nesting depth of each column-A label.

    ABS uses two conventions for the same hierarchy and mixes them across the
    sheets of one cube: a real cell indent (tables 1.1, 10.x) and three leading
    spaces per level inside the string (tables 1.2, 13.x). Reading only one of
    them flattens half the cubes, collapsing "Mean income per week / Lowest
    quintile" and "Income share / Lowest quintile" onto the same key.
    """
    depths = []
    for r in range(1, n_rows + 1):
        cell = worksheet.cell(row=r, column=1)
        text = cell.value if isinstance(cell.value, str) else ""
        leading = len(text) - len(text.lstrip(" "))
        depths.append(int(cell.alignment.indent or 0) + leading // 3)
    return depths


def _blocks(raw) -> list[tuple[int, int]]:
    starts = [
        i
        for i, row in enumerate(raw)
        if row and row[0].lower() == ABS_BOILERPLATE
    ]
    if not starts:
        starts = [0]
    return list(zip(starts, [*starts[1:], len(raw)], strict=True))


def _header_rows(raw, start: int, stop: int) -> range:
    """Header rows are the run after the title whose column A is empty."""
    first = start + 4
    last = first
    while last < stop and raw[last][0] == "":
        last += 1
    return range(first, last)


def _columns(filled, header_rows):
    """Column -> (label path, unit, kind), read from its header labels."""
    out: dict[int, tuple[tuple[str, ...], str, str]] = {}
    for col in range(1, MAX_COL):
        parts: list[str] = []
        unit, kind = "", "estimate"
        for r in header_rows:
            raw_label = filled[r][col] if col < len(filled[r]) else ""
            label = strip_footnotes(raw_label)
            if not label:
                continue
            if is_unit(label):
                unit = label
                if kind_of(label) != "estimate":
                    kind = kind_of(label)
                continue
            if kind_of(label) != "estimate":
                kind = kind_of(label)
                continue
            if not parts or parts[-1] != label:
                parts.append(label)
        if parts or unit:
            out[col] = (tuple(parts), unit, kind)
    return out


def parse_sheet(worksheet, table_id: str, table_name: str) -> SheetParse:
    raw, filled = _grids(worksheet)
    depths = _depths(worksheet, len(raw))
    out = SheetParse(table_id=table_id, table_name=table_name)
    section = -1

    for start, stop in _blocks(raw):
        header_rows = _header_rows(raw, start, stop)
        columns = _columns(filled, header_rows)
        if not columns:
            continue
        section += 1
        section_kind, section_unit, section_measure = "estimate", "", ""
        stack: dict[int, str] = {}

        for i in range(header_rows.stop, stop):
            label, rest = raw[i][0], raw[i][1:]
            clean = strip_footnotes(label)
            if is_banner(clean, rest):
                section += 1
                section_kind, section_unit, section_measure = split_banner(
                    clean
                )
                out.banners.append(clean)
                stack = {}
                continue

            data_cols = [
                c for c in columns if c < len(raw[i]) and raw[i][c] != ""
            ]
            depth = depths[i] if i < len(depths) else 0
            if not data_cols:
                # A group header. Footnotes and notes carry nothing to nest
                # under, so require the row to read like a heading.
                if clean and len(clean) <= 120 and not label.startswith("("):
                    stack = {d: v for d, v in stack.items() if d < depth}
                    stack[depth] = clean
                continue
            if not clean:
                out.dropped_numeric += len(data_cols)
                continue

            path = tuple(stack[d] for d in sorted(stack) if d < depth)
            path = (*path, clean)
            # Where the columns are survey years or categories, the unit sits
            # in column B, which carries no header of its own.
            row_unit = ""
            if 1 not in columns and len(raw[i]) > 1 and is_unit(raw[i][1]):
                row_unit = raw[i][1]

            for col in data_cols:
                value, flag = parse_number(raw[i][col])
                if value is None and not flag:
                    continue
                col_path, col_unit, col_kind = columns[col]
                kind = section_kind
                if col_kind != "estimate":
                    kind = col_kind
                if row_unit:
                    kind = kind_of(row_unit)
                unit = row_unit or col_unit or section_unit
                out.records.append(
                    Record(
                        kind=kind,
                        section=section,
                        row_path=path,
                        column=col,
                        col_path=col_path,
                        unit="" if kind != "estimate" else unit,
                        measure_prefix=section_measure,
                        value=value,
                        flag=flag,
                    )
                )
    return out


def parse_workbook(path: str) -> list[SheetParse]:
    workbook = openpyxl.load_workbook(path, data_only=True)
    parsed = []
    for name in workbook.sheetnames:
        if name.lower().startswith("contents"):
            continue
        worksheet = workbook[name]
        title = ""
        for row in worksheet.iter_rows(min_row=4, max_row=4, values_only=True):
            title = norm(row[0])
        parsed.append(
            parse_sheet(worksheet, name.replace("Table", "").strip(), title)
        )
    workbook.close()
    return parsed


def _ordinals(sheet: SheetParse) -> dict[tuple[str, int], int]:
    """Position of each section within the sections of its own kind."""
    seen: dict[str, list[int]] = {}
    for record in sheet.records:
        sections = seen.setdefault(record.kind, [])
        if record.section not in sections:
            sections.append(record.section)
    return {
        (kind, section): n
        for kind, sections in seen.items()
        for n, section in enumerate(sections)
    }


AMBIGUOUS = object()


def join_sheet(sheet: SheetParse):
    """Yield each estimate with the RSE and margin of error describing it.

    Panels mirror each other, so a cell is matched on its row label path and
    physical column. The paths are not always written identically: an error
    panel may omit the outer group its estimate panel states (table 5.3 drops
    "Proportion of households with characteristic"), or reword it (table 12.3
    says "Superannuation balance" for "Superannuation account balance"). The
    match is therefore made on the longest path *suffix* that identifies one
    estimate in that column, which absorbs both differences without matching
    across genuinely different rows.

    The plain key is used when it identifies an estimate uniquely. It does not
    when a sheet states two estimate panels over the same rows, and there the
    section ordinal is added: the n-th estimate section is mirrored by the n-th
    RSE section.
    """
    ordinals = _ordinals(sheet)
    estimates = [r for r in sheet.records if r.kind == "estimate"]
    plain = {(r.row_path, r.column) for r in estimates}
    by_section = len(plain) != len(estimates)

    def base(record: Record):
        if by_section:
            return (ordinals[(record.kind, record.section)],)
        return ()

    index: dict[tuple, object] = {}
    for estimate in estimates:
        prefix = base(estimate)
        for n in range(1, len(estimate.row_path) + 1):
            key = (*prefix, estimate.column, estimate.row_path[-n:])
            index[key] = AMBIGUOUS if key in index else estimate

    def lookup(record: Record):
        prefix = base(record)
        for n in range(len(record.row_path), 0, -1):
            key = (*prefix, record.column, record.row_path[-n:])
            if key in index:
                found = index[key]
                return None if found is AMBIGUOUS else found
        return None

    errors: dict[int, dict[str, Record]] = {}
    for record in sheet.records:
        if record.kind == "estimate":
            continue
        estimate = lookup(record)
        if estimate is None:
            continue
        errors.setdefault(id(estimate), {}).setdefault(record.kind, record)

    for estimate in estimates:
        found = errors.get(id(estimate), {})
        yield estimate, found.get("rse"), found.get("moe")
