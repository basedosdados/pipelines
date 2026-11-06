"""Parsers for the TEC's three spreadsheet layouts.

``ScrutinyEventScreen``
    The per-division Hare-Clark count export, published as XLSX for 2024 and 2025
    only (2021 and earlier are PDF). Rows alternate "Transferred" and "Progress
    Totals" for each count; columns are candidates grouped by party, with a
    ``<PARTY> Totals`` column after each group that must not be read as a
    candidate. This is the wide form the long ``distribution_of_preferences``
    table is reshaped from.

First preferences by polling place
    One sheet per division, rows are candidates and party subtotals, columns are
    polling places. Transposed relative to the Legislative Council's HTML table,
    which puts polling places in rows.

Polling place details
    One sheet per division, a plain header-and-rows list of venues.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

import openpyxl

from pipelines.datasets.au_tas_tec_elections import parse_html

TOTALS_COL = re.compile(r"\bTotals?$", re.I)


def _is_candidate_label(label: str) -> bool:
    """A candidate column header is a surname in caps.

    Summary columns ("Transfer Value", "Total Ballot Papers Counted") are title
    case. The test allows the single lowercase letter in McLENNAN and O'BYRNE,
    which an ``isupper()`` check would reject.
    """
    return not re.search(r"[a-z]{2}", label)


def _cell(value: object) -> str:
    return "" if value is None else str(value).strip()


def _num(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return int(value)
    text = str(value).replace(",", "").strip()
    if not text or not re.fullmatch(r"-?\d+", text):
        return None
    return int(text)


# --------------------------------------------------------------------------------------
# Hare-Clark count export
# --------------------------------------------------------------------------------------


@dataclass
class ScrutinyCount:
    count_number: str
    transferred: dict[str, int | None]
    totals: dict[str, int | None]
    remarks: str = ""


@dataclass
class Scrutiny:
    candidates: list[tuple[str, str]] = field(default_factory=list)
    counts: list[ScrutinyCount] = field(default_factory=list)
    quota: int | None = None
    formal: int | None = None
    informal: int | None = None


def parse_scrutiny(path) -> Scrutiny:
    """Read the wide count export into per-count candidate transfers and totals."""
    wb = openpyxl.load_workbook(path, data_only=True, read_only=True)
    try:
        ws = wb["ScrutinyEventScreen"]
        rows = [list(r) for r in ws.iter_rows(values_only=True)]
    finally:
        wb.close()

    res = Scrutiny()
    # The header row is the one whose second cell reads "Count"; the party labels
    # sit on the row immediately above it. Searching for a row containing
    # "Totals" finds the header itself, because the roll-up columns are named
    # "<PARTY> Totals" — the party row must be located by position, not content.
    header_idx = next(
        (
            i
            for i, r in enumerate(rows[:12])
            if _cell(r[1] if len(r) > 1 else "") == "Count"
        ),
        None,
    )
    if header_idx is None or header_idx == 0:
        return res
    header_row = rows[header_idx]
    group_row = rows[header_idx - 1]

    # The sheet is two tables side by side sharing one header row. Table I holds
    # the per-count transfers, Table II the running totals, and the whole
    # candidate block is repeated for the second. Each block is also followed by
    # summary columns ("<PARTY> Totals", "Transfer Value", "Total Ballot Papers
    # Counted") that are not candidates. Reading only the first block leaves every
    # running total NULL, because the Progress Totals row writes into Table II.
    party = ""
    blocks: list[list[tuple[int, str, str]]] = [[]]
    seen: set[str] = set()
    for idx, raw in enumerate(header_row):
        label = _cell(raw)
        if not label or idx < 3:
            continue
        group_label = _cell(group_row[idx]) if idx < len(group_row) else ""
        if group_label:
            party = group_label
        if TOTALS_COL.search(label) or not _is_candidate_label(label):
            continue
        if label in seen:
            # The repeat marks the start of Table II.
            blocks.append([])
            seen = set()
        seen.add(label)
        blocks[-1].append((idx, label, party))

    first_block = blocks[0]
    second_block = blocks[1] if len(blocks) > 1 else blocks[0]
    res.candidates = [(label, group) for _, label, group in first_block]

    # Formal, informal and quota are read from the HTML fragments instead, which
    # publish them as plain labelled values. Here they are scattered across merged
    # cells in a rendered formula ("Quota = --- + 1 = 8432") and are not worth
    # recovering twice.

    pending: ScrutinyCount | None = None
    for row in rows:
        number = _cell(row[1] if len(row) > 1 else "")
        kind = _cell(row[2] if len(row) > 2 else "")
        if kind == "Transferred":
            pending = ScrutinyCount(
                count_number=number,
                transferred={
                    label: _num(row[idx]) if idx < len(row) else None
                    for idx, label, _ in first_block
                },
                totals={},
            )
        elif kind == "Progress Totals" and pending is not None:
            pending.totals = {
                label: _num(row[idx]) if idx < len(row) else None
                for idx, label, _ in second_block
            }
            tail = [
                _cell(c)
                for c in row
                if _cell(c) and not re.fullmatch(r"-?[\d.]+", _cell(c))
            ]
            pending.remarks = tail[-1] if tail else ""
            res.counts.append(pending)
            pending = None
    if pending is not None:
        res.counts.append(pending)
    return res


# --------------------------------------------------------------------------------------
# First preferences by polling place
# --------------------------------------------------------------------------------------


@dataclass
class PollingPlaceResult:
    division: str
    places: list[str] = field(default_factory=list)
    # ballot name -> per-place votes, aligned with ``places``
    candidates: dict[str, list[int | None]] = field(default_factory=dict)
    party_of: dict[str, str] = field(default_factory=dict)
    formal: list[int | None] = field(default_factory=list)
    informal: list[int | None] = field(default_factory=list)
    total: list[int | None] = field(default_factory=list)


def _is_candidate(label: str) -> bool:
    """Tell a candidate row from a party subtotal row.

    Both sit in the same column. A comma alone does not separate them — the party
    "Shooters, Fishers, Farmers TAS" has two. Candidates are "SURNAME, Given" with
    the surname in caps, so the test is on the surname's casing; it must tolerate
    the single lowercase letter in names like McLENNAN, which an ``isupper()``
    check rejects.
    """
    head, sep, tail = label.partition(",")
    if not sep or not tail.strip():
        return False
    return not re.search(r"[a-z]{2}", head)


def parse_polling_place_results(path) -> dict[str, PollingPlaceResult]:
    """Read a first-preferences-by-polling-place workbook, one sheet per division.

    Candidate rows are interleaved with party subtotal rows carrying the party's
    name in the same column. A subtotal row is identified by not matching the
    "SURNAME, Given" shape every candidate row uses; the party name it carries is
    then attached to the candidates above it.
    """
    wb = openpyxl.load_workbook(path, data_only=True, read_only=True)
    out: dict[str, PollingPlaceResult] = {}
    try:
        for sheet in wb.sheetnames:
            ws = wb[sheet]
            rows = [list(r) for r in ws.iter_rows(values_only=True)]
            header_idx = next(
                (
                    i
                    for i, r in enumerate(rows[:6])
                    if _cell(r[0] if r else "").lower().startswith("candidate")
                ),
                None,
            )
            if header_idx is None:
                continue
            header = rows[header_idx]
            all_places = [_cell(c) for c in header[1:] if _cell(c)]
            # "Total Ordinary Votes" and the trailing "Total" are roll-ups sitting
            # among the venues, not venues. Their indices are dropped from every
            # row so the remaining columns stay aligned.
            keep = [
                i
                for i, p in enumerate(all_places)
                if not parse_html.is_subtotal(p)
            ]
            places = [all_places[i] for i in keep]
            res = PollingPlaceResult(division=sheet, places=places)
            pending: list[str] = []
            for row in rows[header_idx + 1 :]:
                label = _cell(row[0] if row else "")
                if not label:
                    continue
                raw_values = [_num(c) for c in row[1 : 1 + len(all_places)]]
                values = [
                    raw_values[i] if i < len(raw_values) else None
                    for i in keep
                ]
                low = label.lower()
                if low.startswith("total formal"):
                    res.formal = values
                    continue
                if low.startswith("informal"):
                    res.informal = values
                    continue
                if low.startswith("total ballot") or low.startswith(
                    "total votes"
                ):
                    res.total = values
                    continue
                if _is_candidate(label):
                    res.candidates[label] = values
                    pending.append(label)
                else:
                    # A party subtotal row: it names the group the candidates
                    # listed immediately above it belong to.
                    for name in pending:
                        res.party_of[name] = label
                    pending = []
            out[sheet] = res
    finally:
        wb.close()
    return out


# --------------------------------------------------------------------------------------
# Polling place details
# --------------------------------------------------------------------------------------


def parse_polling_place_list(path) -> list[dict[str, str]]:
    """Read a polling place details workbook into one dict per venue."""
    wb = openpyxl.load_workbook(path, data_only=True, read_only=True)
    out: list[dict[str, str]] = []
    try:
        for sheet in wb.sheetnames:
            ws = wb[sheet]
            rows = [list(r) for r in ws.iter_rows(values_only=True)]
            header_idx = next(
                (
                    i
                    for i, r in enumerate(rows[:6])
                    if any(
                        "pollingplace" in _cell(c).lower().replace(" ", "")
                        for c in r
                    )
                ),
                None,
            )
            if header_idx is None:
                continue
            header = [_cell(c) for c in rows[header_idx]]
            for row in rows[header_idx + 1 :]:
                record = {
                    key: _cell(value)
                    for key, value in zip(header, row, strict=False)
                    if key
                }
                if record.get("PollingPlaceName"):
                    record["_sheet"] = sheet
                    out.append(record)
    finally:
        wb.close()
    return out
