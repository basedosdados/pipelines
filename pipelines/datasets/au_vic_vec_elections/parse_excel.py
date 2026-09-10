"""Excel source family for au_vic_vec_elections.

Everything the Victorian Electoral Commission publishes as a workbook is parsed here:
the 2022 general election and its by-elections (``State/Reports/*.xls``), the 2018
Legislative Assembly voting-centre workbooks, the two-party-preferred summaries for
2014, 2018 and 2022, and the two surviving 2002 Legislative Council provinces.

Three properties of the source drive the design.

**The header is authoritative, the filename is not.** Every workbook carries a report
kind, a print date, an election name, a contest name and — for the two report kinds
that have one — a count stage. Filenames contradict all of these:
``Prahran District-Results by Voting Centre.xls`` holds *State Election 2022* data
reprinted in 2025, ``North-Eastern Victoria Region-Results by Voting Centre.xls`` holds
the *North-Eastern Metropolitan* region, and several files exist twice under names that
differ only by a suffix. :func:`read_header` therefore reads the header of every
workbook and :func:`deduplicate` groups on it, never on the path.

**Subtotal rows look exactly like data rows.** ``Total Ordinary Votes``,
``Total Declaration Votes`` and ``TOTAL ALL VOTE TYPES`` sit in the same column as the
voting-centre names. They are dropped from ``result_voting_centre``; the last of them
is what builds the ``result_district`` first-preference rows.

**Columns are interleaved with blank spacers.** Candidate columns are picked by
non-null header cells, never by position.

The module is pure: no Prefect imports, no network, no writes. ``parse_all`` returns one
DataFrame per table, each already carrying exactly ``schema.column_names(table)`` — with
one deliberate omission. It does not return the ``election`` table: that is assembled in
``utils.py`` from the union of every source family's election ids, so that 2006 and 2010,
which arrive through ``parse_html`` and carry no ``election`` rows of their own, are not
missing from it. This module contributes :func:`election_metadata` to that assembly.
"""

from __future__ import annotations

import glob
import os
import re
from collections import Counter
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any

import pandas as pd

from pipelines.datasets.au_vic_vec_elections import schema
from pipelines.datasets.au_vic_vec_elections.constants import constants

# --------------------------------------------------------------------------------------
# Report kinds
# --------------------------------------------------------------------------------------

KIND_ELECTORATE = "Electorate Results (Voting Centres)"
KIND_ELECTORATE_DETAILS = "Electorate Results (Details)"
KIND_REGION_CONSOLIDATED = "Region Result (Consolidated)"
KIND_REGION_VOTES_RECEIVED = "Region Votes Received (Details)"
KIND_DISTRIBUTION = "Distribution Report"

# ``Region Votes Received (Details)`` shares a prefix with nothing, but
# ``Electorate Results (Details)`` is a prefix-collision hazard with
# ``Electorate Results (Voting Centres)``, so the longer strings are tested first.
_KIND_PREFIXES = (
    KIND_REGION_CONSOLIDATED,
    KIND_REGION_VOTES_RECEIVED,
    KIND_ELECTORATE_DETAILS,
    KIND_ELECTORATE,
    KIND_DISTRIBUTION,
)

# ``Electorate Results (Details)`` is the same contest at a finer grain: it splits each
# voting centre by vote type and numbers the declaration batches (``Absent 1``,
# ``Early Vote 2``). It must therefore share a deduplication group with the
# ``(Voting Centres)`` report of the same contest, or the contest is counted twice.
_KIND_GROUP = {KIND_ELECTORATE_DETAILS: KIND_ELECTORATE}

# Stage preference, used only to break an exact print-timestamp tie: two workbooks
# printed at the same instant differ only in stage, and the recheck is the later count.
# It is NOT the primary sort — see :func:`deduplicate` for why the print date leads.
_STAGE_RANK = {"Recheck": 1, "Primary": 0, "": 0}

GOVERNMENT_LEVEL = "state"
CHAMBER_ASSEMBLY = "legislative_assembly"
CHAMBER_COUNCIL = "legislative_council"
SYSTEM_PREFERENTIAL = "compulsory_preferential"
SYSTEM_STV = "single_transferable_vote"

COUNT_FIRST = "first_preference"
COUNT_2CP = "two_candidate_preferred"
COUNT_2PP = "two_party_preferred"

# Row labels that are subtotals or grand totals, not voting centres.
_SUBTOTAL_LABELS = {
    "total ordinary votes",
    "total declaration votes",
    "total all vote types",
    "district total",
    "region total",
    "ordinary votes",
    "total",
}

# Declaration-vote row labels mapped onto the vote_type vocabulary. ``declaration`` is
# the 2002 spelling of a vote cast at the returning officer's office by declaration
# because the elector could not be found on the roll — a provisional vote in the modern
# vocabulary. The raw label survives in ``voting_centre_name``.
_DECLARATION_VOTE_TYPES = {
    "absent": "absent",
    "early vote": "early",
    "early vote - mobile": "early",
    "early votes": "early",
    "postal vote": "postal",
    "postal votes": "postal",
    "provisional": "provisional",
    "marked as voted": "marked_as_voted",
    "declaration": "provisional",
}

# Header cells of an electorate workbook that are not candidates.
_NON_CANDIDATE_HEADERS = {
    "informal votes",
    "informal",
    "total votes polled",
    "total votes",
    "total",
    "voting centre",
    "mis-sorts",
}

# Trailing columns of a distribution report that are not candidates.
_NON_CANDIDATE_DISTRIBUTION = {
    "gain/loss",
    "exhausted",
    "total",
    "candidates elected at this count",
    "candidates",
}

_2PP_ALP = "ALP"
_2PP_COALITION = "Liberal/National"

# Polling days come from ``constants.ELECTION_META``, never from a workbook: a workbook
# carries only its print date, which is when the report was generated and not when the
# election was held.
#
# Narracan is the one that needs saying out loud. Its election id is
# ``narracan_supp2023`` and its date is 2023-01-28, both keyed to 2023 rather than 2022,
# because the 2022 general-election contest for Narracan District was voided when a
# candidate died during the campaign. Narracan therefore has no 2022 general result at
# all, and its only workbook is the supplementary election held on 28 January 2023 —
# printed some days later, which is the only date the file itself states. The polling
# day is taken from the VEC's by-elections timeline page.


# --------------------------------------------------------------------------------------
# Scalar helpers
# --------------------------------------------------------------------------------------


def slug(text: str) -> str:
    """Lowercase, with every run of non-alphanumerics folded to a single underscore."""
    return re.sub(
        r"_+", "_", re.sub(r"[^a-z0-9]+", "_", str(text).lower())
    ).strip("_")


def grid(frame: pd.DataFrame) -> Any:
    """A positional object view of a sheet.

    Every parser here walks cells by (row, column) position, which ``.iat`` does but
    pandas discourages; a single object array is both faster and lint-clean.
    """
    return frame.to_numpy(dtype=object)


def _text(value: Any) -> str:
    """Collapse a cell to trimmed single-space text; blanks become the empty string."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return re.sub(r"\s+", " ", str(value)).strip()


def _key(value: Any) -> str:
    return _text(value).lower()


def clean_int(value: Any) -> int | None:
    """Parse a vote count. Blank, ``-`` and ``---`` are missing, never zero."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int,)):
        return int(value)
    if isinstance(value, float):
        return round(value)
    text = _text(value).replace(",", "")
    if text in {"", "-", "--", "---", "n/a", "na"}:
        return None
    match = re.fullmatch(r"[+-]?\d+(?:\.0+)?", text)
    if match:
        return int(float(text))
    return None


def clean_float(value: Any) -> float | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    text = _text(value).replace(",", "")
    if text in {"", "-", "--", "---"}:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def clean_percentage(value: Any) -> float | None:
    """Return a percentage on the 0-100 scale.

    The VEC stores the same quantity three ways: ``'(3.34% of total votes)'`` in a
    workbook header, ``'\\n5.88%'`` in a percentage row, and the bare fraction
    ``0.6113`` in the two-party-preferred summaries.
    """
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        number = float(value)
        return number * 100.0 if -1.0 <= number <= 1.0 else number
    match = re.search(r"([\d.]+)\s*%", _text(value))
    if match:
        return float(match.group(1))
    return None


def split_ballot_name(
    ballot_name: str | None,
) -> tuple[str | None, str | None]:
    """Split ``"SURNAME, Given Names"`` on its first comma."""
    if not ballot_name:
        return None, None
    if "," in ballot_name:
        surname, given = ballot_name.split(",", 1)
        surname, given = surname.strip(), given.strip()
        return (surname or None), (given or None)
    return ballot_name.strip() or None, None


def _strip_contest_suffix(name: str) -> str:
    """``"Albert Park District"`` -> ``"Albert Park"``; also Region and Province."""
    return re.sub(
        r"\s+(District|Region|Province)$", "", _text(name), flags=re.I
    ).strip()


def _parse_print_datetime(text: str) -> datetime | None:
    match = re.search(
        r"(\d{2}/\d{2}/\d{4})[,\s]+(\d{1,2}:\d{2}(?::\d{2})?)\s*([AP]M)?",
        text,
        flags=re.I,
    )
    if match is None:
        return None
    day, clock, meridiem = match.groups()
    parts = clock.split(":")
    if len(parts) == 2:
        clock = f"{clock}:00"
    stamp = f"{day} {clock}"
    if meridiem:
        try:
            return datetime.strptime(
                f"{stamp} {meridiem.upper()}", "%d/%m/%Y %I:%M:%S %p"
            )
        except ValueError:
            return None
    try:
        return datetime.strptime(stamp, "%d/%m/%Y %H:%M:%S")
    except ValueError:
        return None


def election_identity(election_name: str) -> tuple[str, str, int]:
    """Map a header election name to ``(election_id, election_type, year)``."""
    name = _text(election_name)
    general = re.fullmatch(r"State Election (\d{4})", name, flags=re.I)
    if general:
        year = int(general.group(1))
        return f"state{year}", "state_general", year
    special = re.fullmatch(
        r"(.+?)\s+District\s+(By-election|Supplementary Election)\s+(\d{4})",
        name,
        flags=re.I,
    )
    if special:
        district, kind, year_text = special.groups()
        year = int(year_text)
        marker = "by" if kind.lower() == "by-election" else "supp"
        return f"{slug(district)}_{marker}{year}", "state_by_election", year
    raise ValueError(
        f"unrecognised election name in workbook header: {name!r}"
    )


def election_date(election_id: str) -> date | None:
    """Polling day for one election, from ``constants.ELECTION_META``."""
    entry = constants.ELECTION_META.value.get(election_id)
    return date.fromisoformat(entry[2]) if entry else None


# The directory under the blob container that each election's workbooks live in. It is
# the election's ``source_url``: a per-file URL would name one arbitrary workbook out of
# the hundred that make up a general election.
#
# Only the elections this source family actually covers appear here — 2006 and 2010 are
# published as HTML and reach the dataset through ``parse_html``, so they are absent by
# design. ``parse_all_with_report`` checks this declaration against the files it finds
# in both directions, so an election that appears without a declaration, a declaration
# whose workbooks have moved, and a declaration nothing on disk backs any more all
# raise instead of shipping a stale URL.
_ELECTION_SOURCE_DIRECTORIES: dict[str, str] = {
    "state2002": "historical-results/general",
    "state2014": "historical-results/state2014/files",
    "state2018": "historical-results/state2018/files",
    "state2022": "State/Reports",
    "narracan_supp2023": "State/Reports",
    "prahran_by2025": "State/Reports",
    "nepean_by2026": "State/Reports",
}


def election_metadata() -> dict[str, dict[str, Any]]:
    """Per-election metadata for every election this source family covers.

    ``parse_all`` deliberately does **not** return an ``election`` table. That table is
    assembled centrally in ``utils.py`` from the union of every source family's election
    ids, because 2006 and 2010 arrive through ``parse_html``, which emits no ``election``
    rows, and would otherwise be missing from it. This function is what that assembly
    consumes from here.

    Keyed by ``election_id``; each value carries exactly the columns of the ``election``
    table, so a row can be built from it without further derivation. Pure: it reads the
    declarations above and in ``constants.ELECTION_META``, and touches no file.
    """
    rows: dict[str, dict[str, Any]] = {}
    for election_id, directory in _ELECTION_SOURCE_DIRECTORIES.items():
        name, election_type, _ = constants.ELECTION_META.value[election_id]
        derived_id, _, year = election_identity(name)
        if derived_id != election_id:
            raise ValueError(
                f"constants.ELECTION_META names {election_id!r} as {name!r}, which "
                f"identifies as {derived_id!r}"
            )
        rows[election_id] = {
            "year": year,
            "election_id": election_id,
            "election_name": name,
            "election_type": election_type,
            "government_level": GOVERNMENT_LEVEL,
            "election_date": election_date(election_id),
            "source_url": f"{constants.BLOB_CONTAINER.value}/{directory}",
        }
    return rows


# --------------------------------------------------------------------------------------
# Header reading and deduplication
# --------------------------------------------------------------------------------------


@dataclass(frozen=True)
class ReportHeader:
    """The authoritative identity of one workbook, read from its own first rows."""

    path: str
    rel_path: str
    kind: str
    raw_kind: str
    election_name: str
    election_id: str
    election_type: str
    year: int
    contest_name: str
    stage: str
    print_datetime: datetime | None
    # ``ExcelFile.sheet_names`` is ``list[int | str]``: pandas admits a positional
    # sheet key alongside a named one. Every workbook here is named, but the field is
    # only ever handed back to ``ExcelFile.parse``, which takes either — so the type
    # is widened to match the source rather than asserting a name we never check.
    sheet_names: tuple[int | str, ...]

    @property
    def contest_base(self) -> str:
        return _strip_contest_suffix(self.contest_name)

    @property
    def contest_id(self) -> str:
        return f"{self.election_id}-{slug(self.contest_base)}"

    @property
    def source_url(self) -> str:
        return f"{constants.BLOB_CONTAINER.value}/{self.rel_path.replace(os.sep, '/')}"

    @property
    def group_key(self) -> tuple[str, str, str]:
        return (
            self.election_name,
            self.contest_name,
            _KIND_GROUP.get(self.kind, self.kind),
        )


def read_header(path: str, input_root: str) -> ReportHeader:
    """Read one workbook's header block. Raises when the workbook is unrecognised."""
    excel = pd.ExcelFile(path)
    frame = excel.parse(
        excel.sheet_names[0], header=None, dtype=object, nrows=14
    )
    lines: list[str] = []
    for position in range(len(frame)):
        for value in frame.iloc[position]:
            text = _text(value)
            if text:
                lines.append(text)

    kind = ""
    for candidate in _KIND_PREFIXES:
        if any(line.startswith(candidate) for line in lines):
            kind = candidate
            break
    if not kind:
        raise ValueError(f"{path}: no recognised report kind in the header")

    election_name = ""
    for line in lines:
        if re.fullmatch(
            r"(State Election \d{4}|.+ (By-election|Supplementary Election) \d{4})",
            line,
            flags=re.I,
        ):
            election_name = line
            break
    if not election_name:
        raise ValueError(f"{path}: no election name in the header")

    contest_name, stage = "", ""
    for line in lines:
        staged = re.fullmatch(
            r"(.+?)\s*\((Primary|Recheck)\)", line, flags=re.I
        )
        if staged:
            contest_name, stage = (
                staged.group(1).strip(),
                staged.group(2).capitalize(),
            )
            break
    if not contest_name:
        for line in lines:
            if line == election_name:
                continue
            if re.search(r"\b(District|Region|Province)$", line):
                contest_name = line
                break
    if not contest_name:
        raise ValueError(f"{path}: no contest name in the header")

    election_id, election_type, year = election_identity(election_name)
    print_datetime = None
    for line in lines:
        if "print date" in line.lower():
            print_datetime = _parse_print_datetime(line)
            if print_datetime:
                break
    if print_datetime is None:
        joined = " ".join(lines)
        print_datetime = _parse_print_datetime(joined)

    return ReportHeader(
        path=path,
        rel_path=os.path.relpath(path, input_root),
        kind=_KIND_GROUP.get(kind, kind),
        raw_kind=kind,
        election_name=election_name,
        election_id=election_id,
        election_type=election_type,
        year=year,
        contest_name=contest_name,
        stage=stage,
        print_datetime=print_datetime,
        sheet_names=tuple(excel.sheet_names),
    )


def deduplicate(
    headers: Sequence[ReportHeader],
) -> tuple[list[ReportHeader], list[tuple[ReportHeader, ReportHeader]]]:
    """Keep one workbook per (election, contest, report kind).

    **The latest print date wins.** The count stage breaks an exact timestamp tie
    (``Recheck`` over ``Primary``), and the path breaks a remaining tie so the choice is
    deterministic.

    Ranking by stage first is the obvious rule and it is wrong. Ripon 2018 is the
    counter-example: ``recheckforripondistrict.xls`` is a ``Recheck`` printed
    2018-12-07, and ``ripondistrict.xlsx`` is a ``Primary`` printed three days later on
    2018-12-10 carrying the VEC's published final figure (40,291 formal votes, against
    the recheck's 40,065). A stage-first rule keeps the superseded count. The recheck of
    a *district* is one step in the count, not the end of it; only the print date orders
    the workbooks by when the VEC actually last stated the result.

    The stage tie-break still matters where it always did: the 2022 Legislative Council
    region rechecks (South-Eastern Metropolitan, Northern Victoria, North-Eastern
    Metropolitan) win under this rule too, because they are also the later print.

    Returns the kept headers and the ``(discarded, kept)`` pairs.
    """
    groups: dict[tuple[str, str, str], list[ReportHeader]] = {}
    for header in headers:
        groups.setdefault(header.group_key, []).append(header)

    kept: list[ReportHeader] = []
    dropped: list[tuple[ReportHeader, ReportHeader]] = []
    for _, members in sorted(groups.items()):
        ranked = sorted(
            members,
            key=lambda h: (
                h.print_datetime or datetime.min,
                _STAGE_RANK.get(h.stage, 0),
                h.rel_path,
            ),
            reverse=True,
        )
        winner = ranked[0]
        kept.append(winner)
        dropped.extend((loser, winner) for loser in ranked[1:])
    return kept, dropped


# --------------------------------------------------------------------------------------
# Contest metadata
# --------------------------------------------------------------------------------------


def contest_fields(
    header_or_id: ReportHeader | str,
    *,
    year: int | None = None,
    election_id: str | None = None,
    district_name: str | None = None,
    chamber: str = CHAMBER_ASSEMBLY,
    contest_type: str = "state_district",
    voting_system: str = SYSTEM_PREFERENTIAL,
) -> dict[str, Any]:
    """Build the nine-column contest block shared by the six fact tables."""
    if isinstance(header_or_id, ReportHeader):
        year = header_or_id.year
        election_id = header_or_id.election_id
        district_name = header_or_id.contest_base
        contest_id = header_or_id.contest_id
    else:
        assert election_id is not None and district_name is not None
        contest_id = f"{election_id}-{slug(district_name)}"
    return {
        "year": year,
        "election_id": election_id,
        "contest_id": contest_id,
        "chamber": chamber,
        "government_level": GOVERNMENT_LEVEL,
        "contest_type": contest_type,
        "voting_system": voting_system,
        "district_name": district_name,
        "state_electoral_division_id": None,
    }


def _council_contest(header: ReportHeader) -> dict[str, Any]:
    return contest_fields(
        header,
        chamber=CHAMBER_COUNCIL,
        contest_type="state_region",
        voting_system=SYSTEM_STV,
    )


# --------------------------------------------------------------------------------------
# Electorate Results (Voting Centres)
# --------------------------------------------------------------------------------------


@dataclass
class CandidateColumn:
    """One candidate's column in a workbook, with whatever the header carries."""

    index: int
    ballot_name: str
    party_name: str | None
    group_letter: str | None = None
    position: int | None = None
    is_above_the_line: bool = False


def _parse_stacked_candidate(cell: Any) -> tuple[str, str | None] | None:
    """Split ``"SURNAME, Given\\nParty\\n"`` into ``(ballot_name, party)``.

    The 2002 workbooks sometimes break the line immediately after the comma, so the
    cell reads ``"WILSON, \\nDerek\\n\\nDEMOCRATS"``. Taking the first line as the name
    would give the surname alone and fold the given name into the party, so a name that
    still ends on its comma absorbs the next line.
    """
    raw = (
        ""
        if cell is None or (isinstance(cell, float) and pd.isna(cell))
        else str(cell)
    )
    parts = [re.sub(r"\s+", " ", part).strip() for part in raw.split("\n")]
    parts = [part for part in parts if part]
    if not parts:
        return None
    if _key(parts[0]) in _NON_CANDIDATE_HEADERS:
        return None
    ballot_name = parts.pop(0)
    while ballot_name.endswith(",") and parts:
        ballot_name = f"{ballot_name} {parts.pop(0)}"
    party = " ".join(parts).strip() or None
    return ballot_name, party


def _electorate_header_row(frame: pd.DataFrame) -> int:
    for position in range(min(24, len(frame))):
        row = frame.iloc[position]
        if any(_key(value).startswith("informal") for value in row) and any(
            _key(value).startswith("total") for value in row
        ):
            return position
    raise ValueError("no candidate header row found (no Informal/Total pair)")


def _label_column(
    frame: pd.DataFrame, header_row: int, first_candidate: int
) -> int:
    header = frame.iloc[header_row]
    for position in range(first_candidate):
        if _key(header.iloc[position]) == "voting centre":
            return position
    body = frame.iloc[header_row + 1 :]
    best, best_count = 0, -1
    for position in range(max(first_candidate, 1)):
        count = sum(1 for value in body.iloc[:, position] if _text(value))
        if count > best_count:
            best, best_count = position, count
    return best


def _electorate_totals(frame: pd.DataFrame) -> dict[str, Any]:
    """Read the ENROLMENT / FORMAL / INFORMAL / TOTAL block above the candidate table."""
    wanted = {
        "enrolment": "enrolment",
        "total enrolment": "enrolment",
        "formal votes": "votes_formal",
        "informal votes": "votes_informal",
        "total votes": "votes_total",
    }
    out: dict[str, Any] = {}
    cells = grid(frame)
    for position in range(min(14, len(frame))):
        label = _key(cells[position, 0]).rstrip(": ").strip()
        field_name = wanted.get(label)
        if field_name is None:
            continue
        row = frame.iloc[position]
        number, percentage = None, None
        for value in row[1:]:
            if number is None:
                number = clean_int(value)
                if number is not None:
                    continue
            if percentage is None and "%" in _text(value):
                percentage = clean_percentage(value)
        out[field_name] = number
        if field_name == "votes_informal":
            out["percentage_informal"] = percentage
        elif field_name == "votes_total":
            out["percentage_turnout"] = percentage
    return out


def parse_electorate_workbook(
    header: ReportHeader,
    *,
    contest: dict[str, Any] | None = None,
) -> dict[str, list[dict[str, Any]]]:
    """Parse one single-member voting-centre workbook.

    Produces the contest's ``enrolment_turnout`` row, its ``candidate`` rows, its
    ``result_district`` first-preference rows (built from ``TOTAL ALL VOTE TYPES``) and
    its ``result_voting_centre`` rows. Subtotal rows are dropped.
    """
    if header.raw_kind == KIND_ELECTORATE_DETAILS:
        # Only reachable if such a report ever wins deduplication. Its rows are keyed by
        # (voting centre, numbered vote-type batch) rather than by voting centre, so
        # feeding it through this parser would silently mis-key the output. Every one of
        # the four that exist today loses to a later "(Voting Centres)" print.
        raise ValueError(
            f"{header.rel_path}: 'Electorate Results (Details)' won deduplication for "
            f"{header.contest_name}; its vote-type grain is not handled here"
        )
    excel = pd.ExcelFile(header.path)
    frame = excel.parse(header.sheet_names[0], header=None, dtype=object)
    cells = grid(frame)
    contest = contest or contest_fields(header)

    header_row = _electorate_header_row(frame)
    header_cells = frame.iloc[header_row]
    candidates: list[CandidateColumn] = []
    informal_column: int | None = None
    total_column: int | None = None
    for position, value in enumerate(header_cells):
        key = _key(value).replace("\n", " ")
        if key.startswith("informal"):
            informal_column = position
            continue
        if key.startswith("total votes") or key == "total":
            total_column = position
            continue
        parsed = _parse_stacked_candidate(value)
        if parsed is None:
            continue
        candidates.append(
            CandidateColumn(
                index=position,
                ballot_name=parsed[0],
                party_name=parsed[1],
                position=len(candidates) + 1,
            )
        )
    if not candidates:
        raise ValueError(
            f"{header.rel_path}: candidate header row carries no candidates"
        )

    label_column = _label_column(frame, header_row, candidates[0].index)

    candidate_rows: list[dict[str, Any]] = []
    for column in candidates:
        surname, given = split_ballot_name(column.ballot_name)
        candidate_rows.append(
            {
                **contest,
                "ballot_position": str(column.position),
                "ballot_name": column.ballot_name,
                "candidate_surname": surname,
                "candidate_given_names": given,
                "party_name": column.party_name,
                "group_letter": None,
                "is_elected": None,
                "elected_order": None,
            }
        )

    centre_rows: list[dict[str, Any]] = []
    district_totals: dict[int, int | None] = {}
    percentages: dict[int, float | None] = {}
    for position in range(header_row + 1, len(frame)):
        label = _text(cells[position, label_column])
        if not label:
            continue
        key = label.lower()
        if key.startswith("percentage"):
            for column in candidates:
                percentages[column.index] = clean_percentage(
                    cells[position, column.index]
                )
            continue
        if key in _SUBTOTAL_LABELS:
            if key in {"total all vote types", "total"}:
                for column in candidates:
                    district_totals[column.index] = clean_int(
                        cells[position, column.index]
                    )
            continue
        vote_type = _DECLARATION_VOTE_TYPES.get(key, "ordinary")
        for column in candidates:
            centre_rows.append(
                {
                    **contest,
                    "voting_centre_name": label,
                    "vote_type": vote_type,
                    "count_type": COUNT_FIRST,
                    "ballot_position": str(column.position),
                    "ballot_name": column.ballot_name,
                    "party_name": column.party_name,
                    # Single-member contest: the ballot has no groups.
                    "group_letter": None,
                    "votes": clean_int(cells[position, column.index]),
                }
            )

    district_rows = [
        {
            **contest,
            "count_type": COUNT_FIRST,
            "ballot_position": str(column.position),
            "ballot_name": column.ballot_name,
            "party_name": column.party_name,
            "group_letter": None,
            "votes": district_totals.get(column.index),
            "percentage": percentages.get(column.index),
        }
        for column in candidates
    ]

    totals = _electorate_totals(frame)
    turnout_row = {
        **contest,
        "enrolment": totals.get("enrolment"),
        "votes_formal": totals.get("votes_formal"),
        "votes_informal": totals.get("votes_informal"),
        "votes_total": totals.get("votes_total"),
        "percentage_informal": totals.get("percentage_informal"),
        "percentage_turnout": totals.get("percentage_turnout"),
        "quota": None,
        "seats_to_elect": 1,
    }

    unused = informal_column, total_column  # read for shape validation only
    del unused
    return {
        "enrolment_turnout": [turnout_row],
        "candidate": candidate_rows,
        "result_district": district_rows,
        "result_voting_centre": centre_rows,
    }


# --------------------------------------------------------------------------------------
# Region Result (Consolidated) — used only for the per-district enrolment block
# --------------------------------------------------------------------------------------


_CONSOLIDATED_FIELDS = {
    "enrolment": "enrolment",
    "formal votes": "votes_formal",
    "informal votes": "votes_informal",
    "total votes": "votes_total",
}


def parse_region_consolidated(header: ReportHeader) -> list[dict[str, Any]]:
    """Read the per-district enrolment / formal / informal / total block of a region.

    The body of this report is group-level (ATL/BTL per ticket per voting centre) and is
    fully derivable from the candidate-level ``Region Votes Received`` report, so it is
    not turned into rows. Its header block, however, is the only place the region's
    enrolment can be reached: the eleven Legislative Assembly districts that make up a
    region each state their own enrolment here and nowhere else.
    """
    excel = pd.ExcelFile(header.path)
    blocks: list[dict[str, Any]] = []
    for sheet in excel.sheet_names:
        frame = excel.parse(sheet, header=None, dtype=object, nrows=14)
        cells = grid(frame)
        district, values = None, {}
        for position in range(len(frame)):
            text = _text(cells[position, 0])
            if not text:
                continue
            match = re.fullmatch(
                r"(Enrolment|Formal Votes|Informal Votes|Total Votes)\s*:\s*([\d,]+)",
                text,
            )
            if match:
                values[_CONSOLIDATED_FIELDS[match.group(1).lower()]] = int(
                    match.group(2).replace(",", "")
                )
                continue
            if district is None and text.endswith("District"):
                district = text
        if district and values:
            blocks.append({"district": district, **values})
    return blocks


# --------------------------------------------------------------------------------------
# Region Votes Received (Details) — candidate-level Legislative Council votes
# --------------------------------------------------------------------------------------


def _parse_council_column(cell: Any) -> CandidateColumn | None:
    """Interpret one ``Group X\\nParty\\nCandidate`` header cell."""
    raw = (
        ""
        if cell is None or (isinstance(cell, float) and pd.isna(cell))
        else str(cell)
    )
    parts = [re.sub(r"\s+", " ", part).strip() for part in raw.split("\n")]
    parts = [part for part in parts if part]
    if not parts:
        return None
    head = parts[0]
    if head.lower().startswith("total"):
        return None
    group_match = re.fullmatch(r"Group\s+([A-Z]+)", head, flags=re.I)
    if group_match:
        letter = group_match.group(1).upper()
        if len(parts) < 2:
            return None
        if parts[-1].upper() == "TOTAL":
            return None
        if parts[-1].upper() == "ATL":
            party = " ".join(parts[1:-1]).strip() or None
            return CandidateColumn(
                index=-1,
                ballot_name="",
                party_name=party,
                group_letter=letter,
                is_above_the_line=True,
            )
        party = " ".join(parts[1:-1]).strip() or None
        return CandidateColumn(
            index=-1,
            ballot_name=parts[-1],
            party_name=party,
            group_letter=letter,
        )
    if head.lower().startswith("ungrouped"):
        name = parts[-1] if len(parts) > 1 else ""
        if not name or name.upper() == "TOTAL":
            return None
        return CandidateColumn(
            index=-1, ballot_name=name, party_name=None, group_letter=None
        )
    return None


def _votes_received_group_totals(header_cells: pd.Series) -> dict[str, int]:
    """Map each ticket letter to its ``Group X\nTOTAL`` column."""
    totals: dict[str, int] = {}
    for position, value in enumerate(header_cells):
        raw = (
            ""
            if value is None or (isinstance(value, float) and pd.isna(value))
            else str(value)
        )
        parts = [re.sub(r"\s+", " ", part).strip() for part in raw.split("\n")]
        parts = [part for part in parts if part]
        if len(parts) < 2 or parts[-1].upper() != "TOTAL":
            continue
        match = re.fullmatch(r"Group\s+([A-Z]+)", parts[0], flags=re.I)
        if match:
            totals[match.group(1).upper()] = position
    return totals


def _votes_received_columns(header_cells: pd.Series) -> list[CandidateColumn]:
    columns: list[CandidateColumn] = []
    per_group: dict[str | None, int] = {}
    for position, value in enumerate(header_cells):
        parsed = _parse_council_column(value)
        if parsed is None:
            continue
        parsed.index = position
        if not parsed.is_above_the_line:
            per_group[parsed.group_letter] = (
                per_group.get(parsed.group_letter, 0) + 1
            )
            parsed.position = per_group[parsed.group_letter]
        columns.append(parsed)
    return columns


def parse_votes_received(
    header: ReportHeader,
) -> tuple[list[dict[str, Any]], list[CandidateColumn], dict[str, int | None]]:
    """Parse the candidate-level Legislative Council voting-centre report.

    Returns the ``result_voting_centre`` rows, the ballot structure (group, party,
    candidate, position within group) and the ``REGION TOTAL`` line for cross-checking.

    Two source properties force decisions here. Above-the-line ticket votes are a real
    part of the formal count but belong to no candidate, so they are emitted as rows
    with a null ``ballot_name`` and the ticket's party; without them a voting centre's
    votes do not add up to its formal total. And voting-centre names repeat across the
    eleven districts of a region — ``Absent`` eleven times, ``St Kilda`` three times — so
    every name is qualified with its district, following the VEC's own 2002 convention
    (``Clyde (BASS DISTRICT)``).
    """
    excel = pd.ExcelFile(header.path)
    frame = excel.parse(header.sheet_names[0], header=None, dtype=object)
    cells = grid(frame)
    contest = _council_contest(header)

    block_starts = [
        position
        for position in range(len(frame))
        if _text(cells[position, 0]).endswith("District")
        and any(_text(value) for value in frame.iloc[position, 3:])
    ]
    if not block_starts:
        raise ValueError(f"{header.rel_path}: no district blocks found")

    columns = _votes_received_columns(frame.iloc[block_starts[0]])
    if not columns:
        raise ValueError(
            f"{header.rel_path}: header row carries no candidate columns"
        )
    group_totals = _votes_received_group_totals(frame.iloc[block_starts[0]])
    group_members: dict[str, list[int]] = {}
    for column in columns:
        # Both the above-the-line ticket column and the group's candidates count
        # towards the group's declared total.
        if column.group_letter is not None:
            group_members.setdefault(column.group_letter, []).append(
                column.index
            )

    rows: list[dict[str, Any]] = []
    reconciled = 0
    region_total: dict[str, int | None] = {}
    boundaries = [*block_starts, len(frame)]
    for start, stop in zip(block_starts, boundaries[1:], strict=True):
        district = _text(cells[start, 0])
        for position in range(start + 1, stop):
            label = _text(cells[position, 0])
            if not label:
                continue
            key = label.lower()
            if key in _SUBTOTAL_LABELS:
                if key == "region total":
                    region_total = {
                        "votes_formal": clean_int(
                            cells[position, frame.shape[1] - 3]
                        ),
                        "votes_informal": clean_int(
                            cells[position, frame.shape[1] - 2]
                        ),
                        "votes_total": clean_int(
                            cells[position, frame.shape[1] - 1]
                        ),
                    }
                continue
            if label.endswith("District"):
                continue
            vote_type = _DECLARATION_VOTE_TYPES.get(key, "ordinary")
            centre_name = f"{label} ({district})"
            # A blank cell in this report is a zero, not a missing value: on every row
            # the ticket's own TOTAL column equals its above-the-line column plus its
            # candidates only when blanks are read as zero. The reconciliation runs on
            # every row rather than being assumed.
            for letter, member_columns in group_members.items():
                total_column = group_totals.get(letter)
                if total_column is None:
                    continue
                declared = clean_int(cells[position, total_column])
                if declared is None:
                    continue
                summed = sum(
                    clean_int(cells[position, index]) or 0
                    for index in member_columns
                )
                if summed != declared:
                    raise ValueError(
                        f"{header.rel_path}: {centre_name} group {letter} sums to "
                        f"{summed} but the report declares {declared}; a blank cell in "
                        "this report can no longer be read as a zero"
                    )
                reconciled += 1
            for column in columns:
                rows.append(
                    {
                        **contest,
                        "voting_centre_name": centre_name,
                        "vote_type": vote_type,
                        "count_type": COUNT_FIRST,
                        "ballot_position": (
                            None
                            if column.is_above_the_line
                            else str(column.position)
                        ),
                        "ballot_name": None
                        if column.is_above_the_line
                        else column.ballot_name,
                        "party_name": column.party_name,
                        # The only thing that tells one above-the-line row from
                        # another: same contest, same centre, same vote type, and a
                        # null ballot_name on every one of them.
                        "group_letter": column.group_letter,
                        "votes": clean_int(cells[position, column.index]) or 0,
                    }
                )
    print(
        f"[parse_excel]     {header.contest_base}: {reconciled:,} ticket totals reconciled"
    )
    return rows, columns, region_total


# --------------------------------------------------------------------------------------
# Distribution Report — the single transferable vote count
# --------------------------------------------------------------------------------------


@dataclass
class DistributionResult:
    rows: list[dict[str, Any]] = field(default_factory=list)
    first_preferences: list[dict[str, Any]] = field(default_factory=list)
    seats_to_elect: int | None = None
    quota: int | None = None
    votes_formal: int | None = None
    votes_informal: int | None = None
    votes_total: int | None = None
    elected: list[str] = field(default_factory=list)
    final_totals: dict[str, int | None] = field(default_factory=dict)


def _distribution_header_row(frame: pd.DataFrame) -> int:
    for position in range(min(24, len(frame))):
        if _key(frame.iloc[position, 1]) == "count details":
            return position
    raise ValueError("no 'Count Details' header row found")


def parse_distribution(
    header: ReportHeader,
    party_by_name: dict[str, str | None],
    group_by_name: dict[str, str | None] | None = None,
) -> DistributionResult:
    """Parse a Legislative Council distribution of preferences.

    A count occupies one row when it is the first-preference count and three when it is
    a transfer: ``BPs`` carries the ballot papers moved, ``Value`` the votes they are
    worth at the count's transfer value, and ``PTotal`` each candidate's running total.

    This report names candidates and nothing else, so each candidate's ballot group is
    carried in from the Votes Received report through ``group_by_name`` and stamped on
    the ``result_district`` first-preference rows built here.
    """
    group_by_name = group_by_name or {}
    excel = pd.ExcelFile(header.path)
    frame = excel.parse(header.sheet_names[0], header=None, dtype=object)
    cells = grid(frame)
    contest = _council_contest(header)
    result = DistributionResult()

    joined = " ".join(
        _text(value) for value in frame.iloc[:14].to_numpy().ravel()
    )
    for pattern, attribute in (
        (r"Election of\s+(\d+)\s+Member", "seats_to_elect"),
        (r"Quota:\s*([\d,]+)", "quota"),
        (r"Formal Ballot Papers[^:]*:\s*([\d,]+)", "votes_formal"),
        (r"Informal Ballot Papers[^:]*:\s*([\d,]+)", "votes_informal"),
        (r"Total Ballot Papers[^:]*:\s*([\d,]+)", "votes_total"),
    ):
        match = re.search(pattern, joined, flags=re.I)
        if match:
            setattr(result, attribute, int(match.group(1).replace(",", "")))

    header_row = _distribution_header_row(frame)
    header_cells = frame.iloc[header_row]
    elected_column: int | None = None
    candidates: list[CandidateColumn] = []
    for position in range(4, frame.shape[1]):
        text = _text(header_cells.iloc[position])
        if not text:
            continue
        key = text.lower().strip()
        if key in _NON_CANDIDATE_DISTRIBUTION:
            if key.startswith("candidates elected"):
                elected_column = position
            continue
        candidates.append(
            CandidateColumn(
                index=position,
                ballot_name=text,
                party_name=party_by_name.get(text),
                group_letter=group_by_name.get(text),
                position=len(candidates) + 1,
            )
        )
    if not candidates:
        raise ValueError(
            f"{header.rel_path}: distribution header carries no candidates"
        )

    pending: dict[str, Any] | None = None
    buffer: dict[str, dict[int, int | None]] = {}
    seen_first_preferences = False

    def flush() -> None:
        nonlocal pending, buffer
        if pending is None:
            return
        for column in candidates:
            result.rows.append(
                {
                    **contest,
                    "count_number": pending["count_number"],
                    "count_description": pending["count_description"],
                    "transfer_value": pending["transfer_value"],
                    "ballot_name": column.ballot_name,
                    "party_name": column.party_name,
                    "ballot_papers_transferred": buffer.get("BPs", {}).get(
                        column.index
                    ),
                    "votes_transferred": buffer.get("Value", {}).get(
                        column.index
                    ),
                    "votes_progressive_total": buffer.get("PTotal", {}).get(
                        column.index
                    ),
                }
            )
        pending, buffer = None, {}

    for position in range(header_row + 1, len(frame)):
        count_number = clean_int(cells[position, 0])
        marker = _text(cells[position, 3])
        if count_number is not None:
            flush()
            pending = {
                "count_number": str(count_number),
                "count_description": _text(cells[position, 1]) or None,
                "transfer_value": clean_float(cells[position, 2]),
            }
            if not marker:
                # The first-preference count is a single row: every ballot paper is
                # transferred at value one, so papers, votes and the running total all
                # take the same number.
                values = {
                    column.index: clean_int(cells[position, column.index])
                    for column in candidates
                }
                buffer = {
                    "BPs": values,
                    "Value": dict(values),
                    "PTotal": dict(values),
                }
                pending["transfer_value"] = pending["transfer_value"] or 1.0
                if not seen_first_preferences:
                    seen_first_preferences = True
                    for column in candidates:
                        votes = values.get(column.index)
                        result.first_preferences.append(
                            {
                                **contest,
                                "count_type": COUNT_FIRST,
                                "ballot_position": str(column.position),
                                "ballot_name": column.ballot_name,
                                "party_name": column.party_name,
                                "group_letter": column.group_letter,
                                "votes": votes,
                                "percentage": (
                                    None
                                    if votes is None or not result.votes_formal
                                    else round(
                                        100.0 * votes / result.votes_formal, 4
                                    )
                                ),
                            }
                        )
                flush()
                self_elected = (
                    _text(cells[position, elected_column])
                    if elected_column
                    else ""
                )
                _record_elected(result, self_elected, candidates)
                continue
        if marker in {"BPs", "Value", "PTotal"} and pending is not None:
            buffer[marker] = {
                column.index: clean_int(cells[position, column.index])
                for column in candidates
            }
            if marker == "PTotal":
                result.final_totals = {
                    column.ballot_name: buffer["PTotal"].get(column.index)
                    for column in candidates
                }
                if elected_column is not None:
                    _record_elected(
                        result,
                        _text(cells[position, elected_column]),
                        candidates,
                    )
                flush()
    flush()
    return result


def _record_elected(
    result: DistributionResult,
    cell: str,
    candidates: Sequence[CandidateColumn],
) -> None:
    """Pull candidate names out of the ``Candidates elected at this count`` cell.

    The cell holds a comma-joined list of names that themselves contain commas
    (``" HEATH, Renee, McINTOSH, Tom"``), so it is matched against the known candidates
    rather than split.
    """
    if not cell:
        return
    found = [
        (cell.find(column.ballot_name), column.ballot_name)
        for column in candidates
        if column.ballot_name and column.ballot_name in cell
    ]
    for _, name in sorted(found):
        if name not in result.elected:
            result.elected.append(name)


# --------------------------------------------------------------------------------------
# 2002 Legislative Council provinces
# --------------------------------------------------------------------------------------


def _province_candidates(
    frame: pd.DataFrame, header_row: int
) -> list[CandidateColumn]:
    columns: list[CandidateColumn] = []
    for position, value in enumerate(frame.iloc[header_row]):
        key = _key(value).replace("\n", " ")
        if (
            key in _NON_CANDIDATE_HEADERS
            or key.startswith("informal")
            or key.startswith("total")
        ):
            continue
        parsed = _parse_stacked_candidate(value)
        if parsed is None:
            continue
        columns.append(
            CandidateColumn(
                index=position,
                ballot_name=parsed[0],
                party_name=parsed[1],
                position=len(columns) + 1,
            )
        )
    return columns


def parse_province_2002(
    path: str, input_root: str
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, Any]]:
    """Parse one of the two surviving 2002 Legislative Council province workbooks.

    Three sheets: ``Primary`` (first preferences by voting centre), ``2CP``
    (two-candidate-preferred by voting centre) and ``Pref Dist`` (a compact distribution
    of preferences on a recount).
    """
    excel = pd.ExcelFile(path)
    rel_path = os.path.relpath(path, input_root)
    primary = excel.parse("Primary", header=None, dtype=object)
    cells = grid(primary)
    province = _text(cells[0, 0]).title()
    contest = contest_fields(
        "state2002",
        year=2002,
        election_id="state2002",
        district_name=_strip_contest_suffix(province),
        chamber=CHAMBER_COUNCIL,
        contest_type="state_province",
        voting_system=SYSTEM_PREFERENTIAL,
    )

    header_row = _electorate_header_row(primary)
    candidates = _province_candidates(primary, header_row)
    label_column = _label_column(primary, header_row, candidates[0].index)

    centre_rows: list[dict[str, Any]] = []
    district_totals: dict[int, int | None] = {}
    percentages: dict[int, float | None] = {}
    primary_names: dict[str, str] = {}
    for position in range(header_row + 1, len(primary)):
        label = _text(cells[position, label_column])
        if not label:
            continue
        key = label.lower()
        if key.startswith("percentage"):
            for column in candidates:
                percentages[column.index] = clean_percentage(
                    cells[position, column.index]
                )
            continue
        if key in _SUBTOTAL_LABELS:
            if key == "total":
                for column in candidates:
                    district_totals[column.index] = clean_int(
                        cells[position, column.index]
                    )
            continue
        vote_type = _DECLARATION_VOTE_TYPES.get(key, "ordinary")
        if vote_type == "ordinary":
            primary_names[label.upper()] = label
        for column in candidates:
            centre_rows.append(
                {
                    **contest,
                    "voting_centre_name": label,
                    "vote_type": vote_type,
                    "count_type": COUNT_FIRST,
                    "ballot_position": str(column.position),
                    "ballot_name": column.ballot_name,
                    "party_name": column.party_name,
                    # The 2002 provinces each returned a single member on a
                    # preferential ballot, so they carry no groups.
                    "group_letter": None,
                    "votes": clean_int(cells[position, column.index]),
                }
            )

    district_rows = [
        {
            **contest,
            "count_type": COUNT_FIRST,
            "ballot_position": str(column.position),
            "ballot_name": column.ballot_name,
            "party_name": column.party_name,
            "group_letter": None,
            "votes": district_totals.get(column.index),
            "percentage": percentages.get(column.index),
        }
        for column in candidates
    ]

    party_by_name = {
        column.ballot_name: column.party_name for column in candidates
    }
    two_candidate_centre, two_candidate_district = _parse_province_2cp(
        excel, contest, party_by_name, primary_names
    )
    centre_rows.extend(two_candidate_centre)
    district_rows.extend(two_candidate_district)

    distribution_rows, elected = _parse_province_distribution(
        excel, contest, party_by_name
    )

    candidate_rows = []
    for column in candidates:
        surname, given = split_ballot_name(column.ballot_name)
        candidate_rows.append(
            {
                **contest,
                "ballot_position": str(column.position),
                "ballot_name": column.ballot_name,
                "candidate_surname": surname,
                "candidate_given_names": given,
                "party_name": column.party_name,
                "group_letter": None,
                "is_elected": None
                if elected is None
                else ("yes" if column.ballot_name == elected else "no"),
                "elected_order": "1"
                if elected and column.ballot_name == elected
                else None,
            }
        )

    totals = _electorate_totals(primary)
    turnout_row = {
        **contest,
        "enrolment": totals.get("enrolment"),
        "votes_formal": totals.get("votes_formal"),
        "votes_informal": totals.get("votes_informal"),
        "votes_total": totals.get("votes_total"),
        "percentage_informal": totals.get("percentage_informal"),
        "percentage_turnout": totals.get("percentage_turnout"),
        "quota": None,
        "seats_to_elect": 1,
    }

    frames = {
        "enrolment_turnout": [turnout_row],
        "candidate": candidate_rows,
        "result_district": district_rows,
        "result_voting_centre": centre_rows,
        "distribution_of_preferences": distribution_rows,
    }
    meta = {
        "rel_path": rel_path,
        "contest_id": contest["contest_id"],
        "district_name": contest["district_name"],
        "votes_formal": totals.get("votes_formal"),
        "first_preference_sum": sum(
            v for v in district_totals.values() if v is not None
        ),
    }
    return frames, meta


def _parse_province_2cp(
    excel: pd.ExcelFile,
    contest: dict[str, Any],
    party_by_name: dict[str, str | None],
    primary_names: dict[str, str],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Parse the 2002 ``2CP`` sheet.

    The sheet spells voting-centre names in upper case where ``Primary`` spells them in
    title case; the upper-case form is mapped back so the two counts share a key. The
    ``Mis-sorts`` column is a reconciliation artefact, not a candidate, and is dropped.
    """
    if "2CP" not in excel.sheet_names:
        return [], []
    frame = excel.parse("2CP", header=None, dtype=object)
    cells = grid(frame)
    header_row = _electorate_header_row(frame)
    candidates = _province_candidates(frame, header_row)
    label_column = _label_column(frame, header_row, candidates[0].index)

    centre_rows: list[dict[str, Any]] = []
    totals: dict[int, int | None] = {}
    percentages: dict[int, float | None] = {}
    for position in range(header_row + 1, len(frame)):
        label = _text(cells[position, label_column])
        if not label:
            continue
        key = label.lower()
        if key.startswith("percentage"):
            for column in candidates:
                percentages[column.index] = clean_percentage(
                    cells[position, column.index]
                )
            continue
        if key in _SUBTOTAL_LABELS:
            if key == "total":
                for column in candidates:
                    totals[column.index] = clean_int(
                        cells[position, column.index]
                    )
            continue
        vote_type = _DECLARATION_VOTE_TYPES.get(key, "ordinary")
        name = primary_names.get(label.upper(), label)
        for column in candidates:
            centre_rows.append(
                {
                    **contest,
                    "voting_centre_name": name,
                    "vote_type": vote_type,
                    "count_type": COUNT_2CP,
                    "ballot_position": str(column.position),
                    "ballot_name": column.ballot_name,
                    "party_name": party_by_name.get(column.ballot_name),
                    "group_letter": None,
                    "votes": clean_int(cells[position, column.index]),
                }
            )
    district_rows = [
        {
            **contest,
            "count_type": COUNT_2CP,
            "ballot_position": str(column.position),
            "ballot_name": column.ballot_name,
            "party_name": party_by_name.get(column.ballot_name),
            "group_letter": None,
            "votes": totals.get(column.index),
            "percentage": percentages.get(column.index),
        }
        for column in candidates
    ]
    return centre_rows, district_rows


def _parse_province_distribution(
    excel: pd.ExcelFile,
    contest: dict[str, Any],
    party_by_name: dict[str, str | None],
) -> tuple[list[dict[str, Any]], str | None]:
    """Parse the compact 2002 ``Pref Dist`` sheet.

    Its layout is a small matrix: one column per candidate, one row per count. Only the
    first-preference row and the transfer rows carry numbers; the running total is
    computed, because the sheet states it only once at the end (as ``FINAL TOTAL`` or
    ``Progressive Total``). An excluded candidate's cell is left blank rather than
    negative, so the exclusion is read out of the row's own wording.
    """
    if "Pref Dist" not in excel.sheet_names:
        return [], None
    frame = excel.parse("Pref Dist", header=None, dtype=object)
    cells = grid(frame)

    header_row: int | None = None
    for position in range(len(frame)):
        if _key(cells[position, 0]).startswith("candidates names"):
            header_row = position
            break
    if header_row is None:
        return [], None

    candidates: list[CandidateColumn] = []
    for position in range(1, frame.shape[1]):
        text = _text(cells[header_row, position])
        if not text or text.lower() == "total":
            continue
        candidates.append(
            CandidateColumn(
                index=position,
                ballot_name=text,
                party_name=party_by_name.get(text),
                position=len(candidates) + 1,
            )
        )

    elected: str | None = None
    running: dict[int, int] = {column.index: 0 for column in candidates}
    excluded: set[int] = set()
    rows: list[dict[str, Any]] = []
    count_number = 0
    for position in range(header_row + 1, len(frame)):
        label = _text(cells[position, 0])
        if not label:
            continue
        lowered = label.lower()
        if lowered.startswith("name of elected"):
            elected = label.split(":", 1)[-1].strip() or None
            continue
        if lowered.startswith("final total") or lowered.startswith(
            "progressive total"
        ):
            continue
        is_first = lowered.startswith("total first preference")
        is_transfer = lowered.startswith("transfer of")
        if not (is_first or is_transfer):
            continue
        count_number += 1
        if is_transfer:
            for column in candidates:
                if re.search(
                    rf"\bof\b.*{re.escape(column.ballot_name)}", label
                ):
                    excluded.add(column.index)
        for column in candidates:
            moved = clean_int(cells[position, column.index])
            if column.index in excluded and not is_first:
                progressive: int | None = 0
            else:
                running[column.index] += moved or 0
                progressive = running[column.index]
            rows.append(
                {
                    **contest,
                    "count_number": str(count_number),
                    "count_description": label,
                    "transfer_value": 1.0,
                    "ballot_name": column.ballot_name,
                    "party_name": column.party_name,
                    "ballot_papers_transferred": moved,
                    "votes_transferred": moved,
                    "votes_progressive_total": progressive,
                }
            )
    return rows, elected


# --------------------------------------------------------------------------------------
# Two-party-preferred summaries
# --------------------------------------------------------------------------------------


_TWO_PARTY_METHODS = {
    "2pp": COUNT_2PP,
    "2cp": COUNT_2CP,
    "preference distribution": COUNT_2CP,
}


def _two_party_sheet(excel: pd.ExcelFile, preferred: str | None) -> int | str:
    """The sheet holding the two-party-preferred summary.

    Returns whatever ``ExcelFile.sheet_names`` yields — ``int | str``, per the note on
    ``ReportHeader.sheet_names`` — and the single caller passes it straight back to
    ``ExcelFile.parse``, which accepts both.
    """
    if preferred and preferred in excel.sheet_names:
        return preferred
    for sheet in excel.sheet_names:
        probe = excel.parse(sheet, header=None, dtype=object, nrows=1)
        if len(probe) and _key(probe.iloc[0, 0]) == "district":
            return sheet
    raise ValueError("no two-party-preferred sheet found")


def parse_two_party_preferred(
    path: str,
    input_root: str,
    *,
    election_id: str,
    year: int,
    sheet: str | None = None,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """Parse a two-party-preferred summary into ``result_district`` rows.

    The 2022 file carries a ``Method`` column taking ``Preference distribution``,
    ``2CP`` and ``2PP``. Only ``2PP`` is a notional throw between two parties that were
    not the final two candidates; the other two are genuine two-candidate-preferred
    counts. ``count_type`` records that distinction, which is as much as the published
    schema can hold — the ``Preference distribution`` / ``2CP`` split is provenance, not
    a different quantity, and is reported by :func:`parse_all` rather than stored.

    Percentages are stored in the source as fractions and are scaled to 0-100 here.
    """
    del input_root
    excel = pd.ExcelFile(path)
    frame = excel.parse(_two_party_sheet(excel, sheet), dtype=object)
    frame.columns = [_text(name) for name in frame.columns]

    def find(*needles: str) -> str:
        for column in frame.columns:
            lowered = column.lower()
            if all(needle in lowered for needle in needles):
                return column
        raise ValueError(f"{path}: no column matching {needles}")

    district_column = find("district")
    alp_votes = find("votes")
    alp_share = None
    coalition_votes = None
    coalition_share = None
    for column in frame.columns:
        lowered = column.lower()
        if ("alp" in lowered or "labor" in lowered) and "%" in lowered:
            alp_share = column
        elif (
            "liberal" in lowered or "coalition" in lowered
        ) and "votes" in lowered:
            coalition_votes = column
        elif (
            "liberal" in lowered or "coalition" in lowered
        ) and "%" in lowered:
            coalition_share = column
    for column in frame.columns:
        lowered = column.lower()
        if ("alp" in lowered or "labor" in lowered) and "votes" in lowered:
            alp_votes = column
    if not (alp_share and coalition_votes and coalition_share):
        raise ValueError(
            f"{path}: could not locate both parties' vote and share columns"
        )
    method_column = "Method" if "Method" in frame.columns else None

    rows: list[dict[str, Any]] = []
    methods: dict[str, int] = {}
    for _, record in frame.iterrows():
        raw_district = _text(record[district_column])
        if not raw_district or raw_district.lower() in {"total", "nan"}:
            continue
        district = re.sub(r"\s*\([^)]*\)\s*$", "", raw_district).strip()
        method = _key(record[method_column]) if method_column else ""
        count_type = _TWO_PARTY_METHODS.get(method, COUNT_2PP)
        methods[method or "(none)"] = methods.get(method or "(none)", 0) + 1
        contest = contest_fields(
            election_id,
            year=year,
            election_id=election_id,
            district_name=district,
        )
        for position, (party, votes_column, share_column) in enumerate(
            (
                (_2PP_ALP, alp_votes, alp_share),
                (_2PP_COALITION, coalition_votes, coalition_share),
            ),
            start=1,
        ):
            rows.append(
                {
                    **contest,
                    "count_type": count_type,
                    "ballot_position": str(position),
                    "ballot_name": None,
                    "party_name": party,
                    # A notional Assembly-district throw between two party blocs:
                    # no group and no candidate.
                    "group_letter": None,
                    "votes": clean_int(record[votes_column]),
                    "percentage": clean_percentage(record[share_column]),
                }
            )
    return rows, methods


# --------------------------------------------------------------------------------------
# Assembly
# --------------------------------------------------------------------------------------


def _frame(rows: Iterable[dict[str, Any]], table: str) -> pd.DataFrame:
    names = schema.column_names(table)
    materialised = list(rows)
    if not materialised:
        return pd.DataFrame(columns=names)
    frame = pd.DataFrame(materialised)
    missing = [name for name in names if name not in frame.columns]
    if missing:
        raise ValueError(f"{table}: rows are missing columns {missing}")
    extra = [name for name in frame.columns if name not in names]
    if extra:
        raise ValueError(f"{table}: rows carry unexpected columns {extra}")
    return frame[names]


def _state_reports(input_root: str) -> list[str]:
    pattern = os.path.join(input_root, "State", "Reports", "**", "*.xls*")
    return sorted(glob.glob(pattern, recursive=True))


def _district_workbooks_2018(input_root: str) -> list[str]:
    directory = os.path.join(
        input_root, "historical-results", "state2018", "files"
    )
    paths = sorted(glob.glob(os.path.join(directory, "*.xls*")))
    keep: list[str] = []
    for path in paths:
        name = os.path.basename(path).lower()
        if name.startswith("2ppvote"):
            continue
        if "region" in name:
            continue
        if name.startswith("preference distribution"):
            continue
        keep.append(path)
    return keep


# --------------------------------------------------------------------------------------
# Exclusion audit
# --------------------------------------------------------------------------------------

# Every workbook under the input root is either consumed, discarded by deduplication, or
# matched by one of these rules. An unmatched workbook raises, so a file the VEC adds
# later cannot be dropped silently.
_EXCLUSION_RULES: tuple[tuple[str, str], ...] = (
    (
        r"(^|/)probe_",
        "exploration artefact written by the download step, not source data",
    ),
    (
        r"^(sr_|e\d\d_)(?!22_2pp)",
        "byte-identical top-level copy of a workbook that is already parsed inside the "
        "directory tree",
    ),
    (
        r"^historical-results/files/",
        "out of scope: 2006, 2010 and 2012 by-election, countback and elected-member "
        "workbooks, plus a second copy of the 2014 region files",
    ),
    (
        r"^historical-results/state2014/files/state2014/",
        "out of scope: 2014 Legislative Council region workbooks",
    ),
    (
        r"^historical-results/state2018/files/.*region",
        "out of scope: 2018 Legislative Council region workbooks, which the HTML source "
        "family covers",
    ),
    (
        r"^historical-results/state2018/files/preference distribution",
        "out of scope: a single Werribee continuation sheet, not a whole contest",
    ),
    (
        r"^historical-results/state2018/files/2ppvote2018\.xlsx$",
        "superseded by 2ppvote2018-final.xlsx",
    ),
    (
        r"^website/",
        # Ownership rule, not a scope judgement: parse_website.py downloads the whole
        # website/ tree, workbooks included, and parses what it needs from it. What
        # lands there are the indicative distributions of preferences for Legislative
        # Assembly districts — the Excel siblings of the dop*.html pages that module
        # already reads. Their number grows as it works, so this module must not raise
        # on files another module owns: the subtree is excluded wholesale rather than
        # file by file. Every path outside website/ still fails fast.
        "owned by parse_website.py, which downloads and parses the whole website/ tree "
        "(its indicative distributions of preferences for 2022 Legislative Assembly "
        "districts are the Excel siblings of the dop2022_*.html pages parsed there)",
    ),
    (
        r"^historical-results/state20(06|10)/",
        "out of scope: 2006 and 2010, which are published as HTML",
    ),
    (
        r"^historical-results/state2014/files/fpvbyvotingcentre",
        "out of scope: the single 2014 Legislative Assembly workbook, which the HTML "
        "source family covers, and which states on its face that a recount superseded "
        "its figures",
    ),
)


def audit_exclusions(
    input_root: str, consumed: set[str], deduplicated: set[str]
) -> list[tuple[str, str]]:
    """Classify every workbook under ``input_root``; raise on anything unaccounted for."""
    excluded: list[tuple[str, str]] = []
    unclassified: list[str] = []
    pattern = os.path.join(input_root, "**", "*.xls*")
    for path in sorted(glob.glob(pattern, recursive=True)):
        relative = os.path.relpath(path, input_root).replace(os.sep, "/")
        if relative in consumed or relative in deduplicated:
            continue
        for expression, reason in _EXCLUSION_RULES:
            if re.search(expression, relative, flags=re.I):
                excluded.append((relative, reason))
                break
        else:
            unclassified.append(relative)
    if unclassified:
        raise ValueError(
            "workbooks neither parsed nor explained: "
            + ", ".join(unclassified)
        )
    return excluded


def parse_all(input_root: str) -> dict[str, pd.DataFrame]:
    """Parse every in-scope workbook under ``input_root``.

    Thin wrapper over :func:`parse_all_with_report` that returns only the tables, so the
    return value is uniformly ``DataFrame``-valued and can be iterated safely.
    """
    frames, _ = parse_all_with_report(input_root)
    return frames


def parse_all_with_report(
    input_root: str,
) -> tuple[dict[str, pd.DataFrame], dict[str, Any]]:
    """Parse every in-scope workbook under ``input_root``, with the audit trail.

    Returns one DataFrame per table, each with exactly ``schema.column_names(table)``,
    plus ``result_voting_centre_2018_xlsx``: the 2018 Legislative Assembly
    voting-centre numbers derived from the workbooks, kept apart so they can be compared
    against the independently derived HTML version instead of being double-counted.
    """
    input_root = os.path.abspath(os.path.expanduser(input_root))
    report: dict[str, Any] = {}

    candidate_rows: list[dict[str, Any]] = []
    turnout_rows: list[dict[str, Any]] = []
    district_rows: list[dict[str, Any]] = []
    centre_rows: list[dict[str, Any]] = []
    distribution_rows: list[dict[str, Any]] = []
    centre_rows_2018: list[dict[str, Any]] = []

    checks: list[str] = []
    skipped: list[tuple[str, str]] = []
    election_paths: dict[str, list[str]] = {}
    attempted = 0

    # ---- 1. read every header ---------------------------------------------------
    paths = _state_reports(input_root)
    print(
        f"[parse_excel] State/Reports: {len(paths)} workbooks; reading headers"
    )
    headers: list[ReportHeader] = []
    for path in paths:
        attempted += 1
        headers.append(read_header(path, input_root))
    kept, dropped = deduplicate(headers)
    report["dedup_dropped"] = [
        (
            loser.rel_path,
            loser.stage,
            loser.print_datetime,
            winner.rel_path,
            winner.stage,
        )
        for loser, winner in dropped
    ]
    print(
        f"[parse_excel] deduplication kept {len(kept)}, discarded {len(dropped)}"
    )

    by_kind: dict[str, list[ReportHeader]] = {}
    for header in kept:
        by_kind.setdefault(header.kind, []).append(header)
        election_paths.setdefault(header.election_id, []).append(
            header.rel_path
        )

    # ---- 2. single-member contests ---------------------------------------------
    for index, header in enumerate(by_kind.get(KIND_ELECTORATE, []), start=1):
        parsed = parse_electorate_workbook(header)
        candidate_rows.extend(parsed["candidate"])
        turnout_rows.extend(parsed["enrolment_turnout"])
        district_rows.extend(parsed["result_district"])
        centre_rows.extend(parsed["result_voting_centre"])
        if index % 20 == 0 or index == len(by_kind.get(KIND_ELECTORATE, [])):
            print(
                f"[parse_excel]   districts {index}/{len(by_kind.get(KIND_ELECTORATE, []))}"
            )

    # ---- 3. Legislative Council regions ----------------------------------------
    consolidated: dict[str, list[dict[str, Any]]] = {}
    for header in by_kind.get(KIND_REGION_CONSOLIDATED, []):
        consolidated[header.contest_id] = parse_region_consolidated(header)
        skipped.append(
            (
                header.rel_path,
                "body not parsed: group-level ATL/BTL per voting centre is fully "
                "derivable from the candidate-level Votes Received report; only its "
                "per-district enrolment block is used",
            )
        )
    print(f"[parse_excel] consolidated region reports: {len(consolidated)}")

    ballot_by_contest: dict[str, list[CandidateColumn]] = {}
    region_totals: dict[str, dict[str, int | None]] = {}
    for header in by_kind.get(KIND_REGION_VOTES_RECEIVED, []):
        rows, columns, totals = parse_votes_received(header)
        centre_rows.extend(rows)
        ballot_by_contest[header.contest_id] = columns
        region_totals[header.contest_id] = totals
        print(
            f"[parse_excel]   {header.contest_base}: {len(rows)} council centre rows"
        )

    for header in by_kind.get(KIND_DISTRIBUTION, []):
        columns = ballot_by_contest.get(header.contest_id, [])
        party_by_name = {
            column.ballot_name: column.party_name
            for column in columns
            if not column.is_above_the_line
        }
        group_by_name = {
            column.ballot_name: column.group_letter
            for column in columns
            if not column.is_above_the_line
        }
        result = parse_distribution(header, party_by_name, group_by_name)
        distribution_rows.extend(result.rows)
        district_rows.extend(result.first_preferences)
        contest = _council_contest(header)

        enrolment = None
        blocks = consolidated.get(header.contest_id, [])
        if blocks:
            enrolment = (
                sum(block.get("enrolment", 0) or 0 for block in blocks) or None
            )
            block_formal = sum(
                block.get("votes_formal", 0) or 0 for block in blocks
            )
            if (
                result.votes_formal
                and abs(block_formal - result.votes_formal) > 0
            ):
                checks.append(
                    f"{header.contest_base}: consolidated recheck formal {block_formal:,} "
                    f"vs distribution formal {result.votes_formal:,} "
                    f"(difference {block_formal - result.votes_formal:+,})"
                )
        turnout_rows.append(
            {
                **contest,
                "enrolment": enrolment,
                "votes_formal": result.votes_formal,
                "votes_informal": result.votes_informal,
                "votes_total": result.votes_total,
                "percentage_informal": (
                    None
                    if not (result.votes_informal and result.votes_total)
                    else round(
                        100.0 * result.votes_informal / result.votes_total, 4
                    )
                ),
                "percentage_turnout": (
                    None
                    if not (enrolment and result.votes_total)
                    else round(100.0 * result.votes_total / enrolment, 4)
                ),
                "quota": result.quota,
                "seats_to_elect": result.seats_to_elect,
            }
        )

        elected_order = {
            name: position for position, name in enumerate(result.elected, 1)
        }
        for column in columns:
            if column.is_above_the_line:
                continue
            surname, given = split_ballot_name(column.ballot_name)
            candidate_rows.append(
                {
                    **contest,
                    "ballot_position": str(column.position),
                    "ballot_name": column.ballot_name,
                    "candidate_surname": surname,
                    "candidate_given_names": given,
                    "party_name": column.party_name,
                    "group_letter": column.group_letter,
                    "is_elected": "yes"
                    if column.ballot_name in elected_order
                    else "no",
                    "elected_order": (
                        str(elected_order[column.ballot_name])
                        if column.ballot_name in elected_order
                        else None
                    ),
                }
            )

        # Quota / seat consistency of the final progressive totals.
        at_quota = sum(
            1
            for value in result.final_totals.values()
            if value is not None
            and result.quota is not None
            and value >= result.quota
        )
        if result.seats_to_elect is not None:
            if len(result.elected) != result.seats_to_elect:
                checks.append(
                    f"{header.contest_base}: {len(result.elected)} candidates recorded as "
                    f"elected but {result.seats_to_elect} seats to fill"
                )
            # Under the single transferable vote the final seat is routinely filled
            # without reaching the quota, so only seats-1 and seats are consistent.
            if at_quota not in {
                result.seats_to_elect - 1,
                result.seats_to_elect,
            }:
                checks.append(
                    f"{header.contest_base}: {at_quota} candidates reach the quota "
                    f"({result.quota:,}) at the final count, which is consistent with "
                    f"neither {result.seats_to_elect} nor {result.seats_to_elect - 1} "
                    "of the seats filled at quota"
                )
        print(
            f"[parse_excel]   {header.contest_base}: {len(result.rows)} distribution rows, "
            f"{len(result.elected)} elected"
        )

    # ---- 4. 2002 provinces ------------------------------------------------------
    province_paths = sorted(
        glob.glob(
            os.path.join(
                input_root,
                "historical-results",
                "general",
                "*province2002.xls",
            )
        )
    )
    for path in province_paths:
        attempted += 1
        frames, meta = parse_province_2002(path, input_root)
        candidate_rows.extend(frames["candidate"])
        turnout_rows.extend(frames["enrolment_turnout"])
        district_rows.extend(frames["result_district"])
        centre_rows.extend(frames["result_voting_centre"])
        distribution_rows.extend(frames["distribution_of_preferences"])
        election_paths.setdefault("state2002", []).append(meta["rel_path"])
        print(f"[parse_excel] 2002 province {meta['district_name']}: parsed")

    # ---- 5. 2018 Legislative Assembly workbooks (compared, not merged) ----------
    paths_2018 = _district_workbooks_2018(input_root)
    print(f"[parse_excel] 2018 district workbooks: {len(paths_2018)}")
    headers_2018: list[ReportHeader] = []
    for path in paths_2018:
        attempted += 1
        headers_2018.append(read_header(path, input_root))
    kept_2018, dropped_2018 = deduplicate(headers_2018)
    report["dedup_dropped_2018"] = [
        (
            loser.rel_path,
            loser.stage,
            loser.print_datetime,
            winner.rel_path,
            winner.stage,
        )
        for loser, winner in dropped_2018
    ]
    formal_2018: dict[str, int | None] = {}
    for index, header in enumerate(kept_2018, start=1):
        parsed = parse_electorate_workbook(header)
        centre_rows_2018.extend(parsed["result_voting_centre"])
        formal_2018[header.contest_id] = parsed["enrolment_turnout"][0][
            "votes_formal"
        ]
        election_paths.setdefault(header.election_id, []).append(
            header.rel_path
        )
        if index % 25 == 0 or index == len(kept_2018):
            print(f"[parse_excel]   2018 districts {index}/{len(kept_2018)}")
    report["contests_2018"] = len(kept_2018)
    report["formal_2018"] = formal_2018

    # ---- 6. two-party-preferred summaries ---------------------------------------
    two_party_sources = [
        (
            os.path.join(
                input_root,
                "historical-results",
                "state2014",
                "files",
                "2ppvote2014.xls",
            ),
            "state2014",
            2014,
            None,
        ),
        (
            os.path.join(
                input_root,
                "historical-results",
                "state2018",
                "files",
                "2ppvote2018-final.xlsx",
            ),
            "state2018",
            2018,
            "2PP",
        ),
        (
            os.path.join(input_root, "e22_2pp.xlsx"),
            "state2022",
            2022,
            "Sheet1",
        ),
    ]
    report["two_party_methods"] = {}
    for path, election_id, year, sheet in two_party_sources:
        if not os.path.exists(path):
            raise FileNotFoundError(
                f"two-party-preferred source missing: {path}"
            )
        attempted += 1
        rows, methods = parse_two_party_preferred(
            path, input_root, election_id=election_id, year=year, sheet=sheet
        )
        district_rows.extend(rows)
        report["two_party_methods"][election_id] = methods
        election_paths.setdefault(election_id, []).append(
            os.path.relpath(path, input_root)
        )
        print(
            f"[parse_excel] two-party-preferred {election_id}: {len(rows)} rows"
        )

    # ---- 7. election metadata cross-check ---------------------------------------
    # No ``election`` table is built here — utils.py assembles it centrally from every
    # source family's ids, so that 2006 and 2010 (HTML only) are not missing from it.
    # What this module owes that assembly is :func:`election_metadata`, and what is
    # checked here is that the declaration still describes the files on disk.
    observed_ids = {header.election_id for header in kept + kept_2018} | set(
        election_paths
    )
    declared = election_metadata()
    undeclared = sorted(observed_ids - set(declared))
    if undeclared:
        raise ValueError(
            "elections parsed but not declared in _ELECTION_SOURCE_DIRECTORIES: "
            + ", ".join(undeclared)
        )
    unobserved = sorted(set(declared) - observed_ids)
    if unobserved:
        raise ValueError(
            "elections declared in _ELECTION_SOURCE_DIRECTORIES but not parsed: "
            + ", ".join(unobserved)
        )
    for election_id in sorted(observed_ids):
        # The directory most of the election's files live in, not a character-wise
        # common prefix: the 2022 two-party-preferred workbook sits at the input root
        # and would otherwise collapse the whole election's URL to the container.
        directories = Counter(
            os.path.dirname(relative)
            for relative in election_paths.get(election_id, [])
            if os.path.dirname(relative)
        )
        prefix = (
            directories.most_common(1)[0][0].replace(os.sep, "/")
            if directories
            else ""
        )
        expected = declared[election_id]["source_url"]
        if f"{constants.BLOB_CONTAINER.value}/{prefix}" != expected:
            raise ValueError(
                f"{election_id}: workbooks now live under {prefix!r}, but "
                f"_ELECTION_SOURCE_DIRECTORIES still declares {expected!r}"
            )
    report["election_metadata"] = {
        election_id: declared[election_id]
        for election_id in sorted(observed_ids)
    }
    print(
        f"[parse_excel] election metadata verified for {len(observed_ids)} elections: "
        + ", ".join(sorted(observed_ids))
    )

    # ---- 8. consistency checks --------------------------------------------------
    frames = {
        "candidate": _frame(candidate_rows, "candidate"),
        "enrolment_turnout": _frame(turnout_rows, "enrolment_turnout"),
        "result_district": _frame(district_rows, "result_district"),
        "result_voting_centre": _frame(centre_rows, "result_voting_centre"),
        "distribution_of_preferences": _frame(
            distribution_rows, "distribution_of_preferences"
        ),
        "result_voting_centre_2018_xlsx": _frame(
            centre_rows_2018, "result_voting_centre"
        ),
    }

    consumed = (
        {header.rel_path.replace(os.sep, "/") for header in kept + kept_2018}
        | {
            os.path.relpath(path, input_root).replace(os.sep, "/")
            for path in province_paths
        }
        | {
            os.path.relpath(path, input_root).replace(os.sep, "/")
            for path, _, _, _ in two_party_sources
        }
    )
    deduplicated = {
        header.rel_path.replace(os.sep, "/")
        for header in headers + headers_2018
    } - consumed
    report["excluded"] = audit_exclusions(input_root, consumed, deduplicated)
    report["consumed"] = len(consumed)

    checks.extend(_check_candidate_names(frames))
    checks.extend(_check_first_preference_totals(frames))
    checks.extend(_check_voting_centre_totals(frames, region_totals))
    checks.extend(_check_distribution_first_count(frames))
    checks.extend(
        _check_frame_against_formal(
            frames["result_voting_centre_2018_xlsx"],
            formal_2018,
            "2018 workbook",
        )
    )
    checks.extend(
        _check_frame_against_formal(
            frames["result_voting_centre"][
                frames["result_voting_centre"]["chamber"] == CHAMBER_ASSEMBLY
            ],
            {
                str(row.contest_id): row.votes_formal
                for row in frames["enrolment_turnout"].itertuples()
                if row.chamber == CHAMBER_ASSEMBLY
            },
            "single-member workbook",
        )
    )
    report["checks"] = checks
    report["attempted"] = attempted
    report["skipped"] = skipped
    _print_report(frames, report)
    return frames, report


def _check_first_preference_totals(
    frames: dict[str, pd.DataFrame],
) -> list[str]:
    """Assert per contest that first-preference votes add up to the formal vote."""
    district = frames["result_district"]
    turnout = frames["enrolment_turnout"]
    if district.empty or turnout.empty:
        return []
    totals = (
        district[district["count_type"] == COUNT_FIRST]
        .groupby("contest_id")["votes"]
        .sum(min_count=1)
    )
    formal = turnout.set_index("contest_id")["votes_formal"]
    messages: list[str] = []
    for contest_id, summed in totals.items():
        expected = formal.get(contest_id)
        if expected is None or pd.isna(expected) or pd.isna(summed):
            continue
        if int(summed) != int(expected):
            messages.append(
                f"{contest_id}: first preferences sum to {int(summed):,} but "
                f"votes_formal is {int(expected):,} (difference {int(summed) - int(expected):+,})"
            )
    return messages


def _check_voting_centre_totals(
    frames: dict[str, pd.DataFrame],
    region_totals: dict[str, dict[str, int | None]],
) -> list[str]:
    """Check the Legislative Council voting-centre rows against the region's own total."""
    centre = frames["result_voting_centre"]
    if centre.empty:
        return []
    council = centre[
        (centre["chamber"] == CHAMBER_COUNCIL)
        & (centre["count_type"] == COUNT_FIRST)
    ]
    messages: list[str] = []
    for contest_id, totals in sorted(region_totals.items()):
        expected = totals.get("votes_formal")
        if expected is None:
            continue
        summed = council[council["contest_id"] == contest_id]["votes"].sum(
            min_count=1
        )
        if pd.isna(summed):
            continue
        if int(summed) != int(expected):
            messages.append(
                f"{contest_id}: voting-centre first preferences sum to {int(summed):,} "
                f"but the report's REGION TOTAL formal is {int(expected):,} "
                f"(difference {int(summed) - int(expected):+,})"
            )
    return messages


def _check_candidate_names(frames: dict[str, pd.DataFrame]) -> list[str]:
    """Flag ballot names that do not read as ``SURNAME, Given``.

    The VEC prints every candidate that way, so an exception is normally a header cell
    whose line breaks were misread rather than an unusual name.
    """
    candidates = frames["candidate"]
    if candidates.empty:
        return []
    shape = (
        candidates["ballot_name"].astype(str).str.fullmatch(r"[^,]+,\s+\S.*")
    )
    odd = sorted(
        set(candidates.loc[~shape.fillna(False), "ballot_name"].dropna())
    )
    if not odd:
        return []
    return [
        f"{len(odd)} ballot name(s) are not of the form 'SURNAME, Given': "
        + ", ".join(repr(name) for name in odd[:10])
    ]


def _check_distribution_first_count(
    frames: dict[str, pd.DataFrame],
) -> list[str]:
    """Check that the distribution's first count adds up to the contest's formal vote.

    This is the check that catches a recount: the 2002 ``Pref Dist`` sheets were printed
    after a recount and do not agree with the ``Primary`` sheet of the same workbook.
    """
    distribution = frames["distribution_of_preferences"]
    turnout = frames["enrolment_turnout"]
    if distribution.empty or turnout.empty:
        return []
    first = distribution[distribution["count_number"] == "1"]
    totals = first.groupby("contest_id")["votes_transferred"].sum(min_count=1)
    formal = turnout.set_index("contest_id")["votes_formal"]
    messages: list[str] = []
    for contest_id, summed in totals.items():
        expected = formal.get(contest_id)
        if expected is None or pd.isna(expected) or pd.isna(summed):
            continue
        if int(summed) != int(expected):
            messages.append(
                f"{contest_id}: the distribution's first count totals {int(summed):,} "
                f"but votes_formal is {int(expected):,} "
                f"(difference {int(summed) - int(expected):+,})"
            )
    return messages


def _check_frame_against_formal(
    frame: pd.DataFrame, formal: dict[str, Any], label: str
) -> list[str]:
    """Check that voting-centre first preferences add up to the contest's formal vote."""
    if frame.empty:
        return []
    subset = frame[frame["count_type"] == COUNT_FIRST]
    totals = subset.groupby("contest_id")["votes"].sum(min_count=1)
    messages: list[str] = []
    for contest_id, summed in totals.items():
        expected = formal.get(contest_id)
        if expected is None or pd.isna(expected) or pd.isna(summed):
            continue
        if int(summed) != int(expected):
            messages.append(
                f"{contest_id} ({label}): voting-centre first preferences sum to "
                f"{int(summed):,} but votes_formal is {int(expected):,} "
                f"(difference {int(summed) - int(expected):+,})"
            )
    return messages


def _print_report(
    frames: dict[str, pd.DataFrame], report: dict[str, Any]
) -> None:
    print("\n[parse_excel] === rows per table ===")
    for name, frame in frames.items():
        if name.startswith("_"):
            continue
        print(f"  {name:34s} {len(frame):>9,}")
    print(
        f"[parse_excel] workbooks attempted: {report['attempted']}, "
        f"consumed: {report['consumed']}"
    )
    families: dict[str, int] = {}
    for _, reason in report["excluded"]:
        families[reason] = families.get(reason, 0) + 1
    for reason, count in sorted(families.items(), key=lambda item: -item[1]):
        print(f"  [excluded] {count:>3} workbook(s): {reason}")
    print(
        f"[parse_excel] deduplication discarded: {len(report['dedup_dropped'])} (2022 family), "
        f"{len(report['dedup_dropped_2018'])} (2018 family)"
    )
    print("[parse_excel] === contests per election ===")
    coverage: dict[tuple[str, str], set[str]] = {}
    for name in (
        "candidate",
        "enrolment_turnout",
        "result_district",
        "result_voting_centre",
    ):
        frame = frames[name]
        if frame.empty:
            continue
        for row in frame[
            ["election_id", "contest_type", "contest_id"]
        ].itertuples():
            coverage.setdefault(
                (row.election_id, row.contest_type), set()
            ).add(row.contest_id)
    for (election_id, contest_type), contests in sorted(coverage.items()):
        print(f"  {election_id:20s} {contest_type:16s} {len(contests):>3}")
    for election_id, methods in sorted(report["two_party_methods"].items()):
        rendered = ", ".join(
            f"{name}={count}" for name, count in sorted(methods.items())
        )
        print(f"[parse_excel] two-party-preferred {election_id}: {rendered}")
    for message in report["checks"]:
        print(f"  [check] {message}")
    if not report["checks"]:
        print("  [check] no mismatches")
