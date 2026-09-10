"""Parse the VEC's historical HTML result pages for the 2006-2018 elections.

The Victorian Electoral Commission publishes 2006, 2010, 2014 and 2018 state
election results as static HTML under ``historical-results/state<YEAR>/``.  Four
page families carry data, one per contest:

===========================  ===================================================
page                         content
===========================  ===================================================
``<contest>``                elected member, enrolment and turnout, first
                             preference count and — for Legislative Assembly
                             districts — the two-candidate-preferred count
``distribution<contest>``    the full distribution of preferences, count by
                             count, as a candidate-by-count matrix
``fpvbyvotingcentre<c>``     first preference votes by voting centre
``tcpbyvotingcentre<c>``     two-candidate-preferred votes by voting centre
===========================  ===================================================

2006 and 2010 prefix every file with ``state<YEAR>`` and name the contest page
``state<YEAR>result<contest>``; 2006 also spells the two-candidate-preferred
voting-centre page ``tcpbyvc`` rather than ``tcpbyvotingcentre``.

Every function here is pure: it reads files and returns data frames, and holds
no Prefect, network or BigQuery dependency.  ``parse_all`` returns one frame per
table, with exactly the columns declared in :mod:`schema`.
"""

from __future__ import annotations

import math
import re
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from io import StringIO
from pathlib import Path

import pandas as pd
from bs4 import BeautifulSoup

from pipelines.datasets.au_vic_vec_elections import schema

# --------------------------------------------------------------------------------------
# Constants
# --------------------------------------------------------------------------------------

HTML_YEARS: tuple[int, ...] = (2006, 2010, 2014, 2018)

#: Legislative Council regions have elected five members each since 2006.
SEATS_PER_REGION = 5
SEATS_PER_DISTRICT = 1

GOVERNMENT_LEVEL = "state"

#: The tables this module fills.  ``election`` and the disclosure tables are fed
#: from other sources.
OUTPUT_TABLES: tuple[str, ...] = (
    "enrolment_turnout",
    "result_district",
    "result_voting_centre",
    "distribution_of_preferences",
    "candidate",
)

#: Filename prefix -> page family.  Order matters: ``tcpbyvotingcentre`` must be
#: tested before ``tcpbyvc`` would swallow it, and ``result`` last because it is
#: the 2006/2010 spelling of the contest page.
_PAGE_PREFIXES: tuple[tuple[str, str], ...] = (
    ("distribution", "distribution"),
    ("fpvbyvotingcentre", "fpv"),
    ("tcpbyvotingcentre", "tcp"),
    ("tcpbyvc", "tcp"),
    ("result", "summary"),
)

#: Declaration-vote row labels, as printed, mapped to the ``vote_type``
#: dictionary.  Wording drifts across years: 2010 regions print ``Postal Vote``
#: and ``Marked as Voted`` where districts print ``Postal Votes`` and ``Marked As
#: Voted Votes``.
_VOTE_TYPE_BY_LABEL: dict[str, str] = {
    "postal votes": "postal",
    "postal vote": "postal",
    "early votes": "early",
    "early vote": "early",
    "absent votes": "absent",
    "absent": "absent",
    "provisional votes": "provisional",
    "provisional": "provisional",
    "marked as voted votes": "marked_as_voted",
    "marked as voted": "marked_as_voted",
    # 2006 publishes neither a provisional nor a marked-as-voted line. It publishes a
    # single "Declaration Votes" line covering both, so it is a coarser category than
    # either and is published under its own value rather than folded into one of them.
    "declaration votes": "declaration",
}

#: Row labels that aggregate other rows and must never become observations.
_AGGREGATE_ROW_LABELS: frozenset[str] = frozenset(
    {
        "ordinary votes total",
        "total",
        # Prahran 2014 and Ripon 2018 were recounted and the VEC replaced the
        # whole voting-centre breakdown with a single district-wide line.
        "all votes votes",
    }
)

_ORDINARY_SUBTOTAL_LABEL = "ordinary votes total"

_FIRST_PREFERENCE_HEADER = "1st pref votes"
#: The two-candidate-preferred result reached by distributing preferences.  It
#: sums exactly to the formal vote and is preferred wherever it exists.
_DISTRIBUTION_HEADER = "votes after distribution"
#: The two-candidate-preferred count conducted on election night, published in
#: place of the distribution result for a seat won on first preferences.  It is
#: the same count reported by the ``tcpbyvotingcentre`` page and so differs from
#: the formal vote by the mis-sorts.  2006 publishes it for every district,
#: alongside the distribution result where there was one; 2010, 2014 and 2018
#: publish exactly one of the two per district.
_PREFERRED_HEADER = "preferred votes"

_FIRST_PREFERENCE_COUNT_LABEL = "total first preference"
_PROGRESSIVE_TOTAL_LABEL = "progressive total"
_FINAL_TOTAL_LABEL = "final total"

_ABSOLUTE_MAJORITY_LABEL = "votes required to constitute an absolute majority"

# Legislative Assembly preferences are always transferred at value one.
_ASSEMBLY_TRANSFER_VALUE = 1.0

_NUMBER_WITH_PERCENT = re.compile(r"^\s*([\d,]+)\s*(?:\(\s*([\d.]+)\s*%)?")
_ELECTED_ORDINAL = re.compile(
    r"(\d+)\s*(?:st|nd|rd|th)\s+elected\s*:\s*", re.I
)
_TRAILING_CONTEST_WORD = re.compile(r"\s+(District|Region|Province)\s*$", re.I)
_NON_ALNUM = re.compile(r"[^a-z0-9]+")


# --------------------------------------------------------------------------------------
# Diagnostics
# --------------------------------------------------------------------------------------


@dataclass
class ParseReport:
    """What the parse attempted, produced and could not use.

    ``failures`` holds files that were expected to yield data and did not.
    ``skipped`` holds files deliberately not used, each with a reason, so that a
    file can never be dropped without a trace.
    """

    files_attempted: int = 0
    files_parsed: int = 0
    failures: list[tuple[str, str]] = field(default_factory=list)
    skipped: list[tuple[str, str]] = field(default_factory=list)
    notes: Counter = field(default_factory=Counter)
    contests: set[tuple[int, str, str]] = field(default_factory=set)

    def note(self, key: str, count: int = 1) -> None:
        self.notes[key] += count


#: Where a contest's two-candidate-preferred count came from.
TCP_FROM_DISTRIBUTION = "votes_after_distribution"
TCP_FROM_ELECTION_NIGHT = "preferred_votes"


@dataclass
class ParseAudit:
    """Values the consistency checks need that are not part of any table."""

    #: contest_id -> sum of the candidate columns on the ``FINAL TOTAL`` row.
    final_totals: dict[str, int] = field(default_factory=dict)
    #: contest_id -> ``TCP_FROM_DISTRIBUTION`` or ``TCP_FROM_ELECTION_NIGHT``.
    tcp_sources: dict[str, str] = field(default_factory=dict)


class ParseError(RuntimeError):
    """Raised when one or more pages could not be parsed as expected."""


# --------------------------------------------------------------------------------------
# Small pure helpers
# --------------------------------------------------------------------------------------


def slugify(name: str) -> str:
    """``"South-Eastern Metropolitan"`` -> ``"south_eastern_metropolitan"``."""
    return _NON_ALNUM.sub("_", name.lower()).strip("_")


def _cell(value: object) -> str | None:
    """Normalise one parsed cell to a stripped string, or ``None``."""
    if value is None:
        return None
    if isinstance(value, float):
        if math.isnan(value):
            return None
        if value.is_integer():
            return str(int(value))
        return repr(value)
    text = re.sub(r"\s+", " ", str(value)).strip()
    if text in ("", "-", "\u2013", "\u2014"):
        return None
    return text


def _to_int(value: str | None) -> int | None:
    if value is None:
        return None
    text = value.replace(",", "").strip()
    if not text or not re.fullmatch(r"-?\d+", text):
        return None
    return int(text)


def _to_percent(value: str | None) -> float | None:
    """``"4.13%"`` -> ``4.13``.  Percentages are stored as numbers 0-100."""
    if value is None:
        return None
    text = value.replace(",", "").replace("%", "").strip()
    if not text or not re.fullmatch(r"-?\d+(?:\.\d+)?", text):
        return None
    return float(text)


def _lower(value: str | None) -> str:
    return value.lower() if value else ""


def _is_repeated_row(row: Sequence[str | None]) -> bool:
    """A navigation row repeats one link's text across every cell."""
    values = [v for v in row if v is not None]
    return len(row) > 1 and len(values) > 1 and len(set(values)) == 1


# --------------------------------------------------------------------------------------
# Reading a page into rectangular grids
# --------------------------------------------------------------------------------------

Grid = list[list[str | None]]


def read_page(path: Path) -> tuple[str | None, list[Grid]]:
    """Return the page's first ``h2`` and every table as a grid of strings.

    ``pandas.read_html`` is used as the HTML-table-to-matrix converter, one
    table at a time, with three normalisations applied first:

    1. ``thead``/``tfoot`` become ``tbody`` and ``th`` becomes ``td``, so pandas
       promotes nothing to a column index and every row is returned in document
       order.  These pages put the candidate names and the party names in two
       ``thead`` rows of unequal width, which no ``header=`` argument survives.
    2. ``colspan``/``rowspan`` are dropped from empty cells.  The pages use an
       empty ``colspan="100"`` cell as a horizontal rule, and an empty
       ``colspan="4"`` cell for an independent candidate's missing party; left
       alone, either silently widens the frame and shifts the data.
    3. Rows that are empty after that are dropped.
    """
    markup = path.read_text(encoding="utf-8", errors="replace")
    soup = BeautifulSoup(markup, "lxml")

    heading = soup.find("h2")
    title = _cell(heading.get_text(" ", strip=True)) if heading else None

    grids: list[Grid] = []
    for table in soup.find_all("table"):
        for wrapper in table.find_all(["thead", "tfoot"]):
            wrapper.name = "tbody"
        for header_cell in table.find_all("th"):
            header_cell.name = "td"
        for cell in table.find_all("td"):
            if not cell.get_text(strip=True):
                cell.attrs.pop("colspan", None)
                cell.attrs.pop("rowspan", None)
        if table.find("tr") is None:
            grids.append([])
            continue
        frame = pd.read_html(StringIO(str(table)), header=None)[0]
        rows = [
            [_cell(v) for v in row]
            for row in frame.itertuples(index=False, name=None)
        ]
        grids.append([r for r in rows if any(v is not None for v in r)])
    return title, grids


# --------------------------------------------------------------------------------------
# Locating the tables of interest within a page
# --------------------------------------------------------------------------------------


def _find_enrolment_grid(grids: Iterable[Grid]) -> Grid | None:
    for grid in grids:
        if not grid or any(len(row) != 2 for row in grid):
            continue
        if any("formal votes" in _lower(row[0]) for row in grid):
            return grid
    return None


def _find_candidate_grids(grids: Iterable[Grid]) -> list[tuple[str, Grid]]:
    """Return ``(header_label, grid)`` for every Candidate/Party result table."""
    found: list[tuple[str, Grid]] = []
    for grid in grids:
        if len(grid) < 2 or len(grid[0]) < 4:
            continue
        header = [_lower(v) for v in grid[0]]
        if header[0] != "candidate" or header[1] != "party":
            continue
        found.append((header[2], grid))
    return found


def _find_distribution_grid(grids: Iterable[Grid]) -> Grid | None:
    for grid in grids:
        if not grid or len(grid[0]) < 3:
            continue
        if grid[0][0] is None and _lower(grid[0][-1]) == "total":
            return grid
    return None


def _find_voting_centre_grid(grids: Iterable[Grid]) -> Grid | None:
    """The candidate-by-voting-centre grid, identified by its two header rows.

    Legislative Council voting-centre pages carry a single header row of party
    groups rather than a candidate row, and are deliberately not matched here.
    """
    for grid in grids:
        if len(grid) < 3 or len(grid[0]) < 3:
            continue
        if grid[0][0] is not None:
            continue
        if _lower(grid[1][0]).startswith("voting centre"):
            return grid
    return None


# --------------------------------------------------------------------------------------
# Contest identity
# --------------------------------------------------------------------------------------


@dataclass(frozen=True)
class Contest:
    """The nine contest-block fields shared by every fact table."""

    year: int
    kind: str  # "district" | "region"
    district_name: str

    @property
    def election_id(self) -> str:
        return f"state{self.year}"

    @property
    def contest_id(self) -> str:
        return f"{self.election_id}-{slugify(self.district_name)}"

    @property
    def is_district(self) -> bool:
        return self.kind == "district"

    def block(self) -> dict[str, object]:
        return {
            "year": self.year,
            "election_id": self.election_id,
            "contest_id": self.contest_id,
            "chamber": (
                "legislative_assembly"
                if self.is_district
                else "legislative_council"
            ),
            "government_level": GOVERNMENT_LEVEL,
            "contest_type": (
                "state_district" if self.is_district else "state_region"
            ),
            "voting_system": (
                "compulsory_preferential"
                if self.is_district
                else "single_transferable_vote"
            ),
            "district_name": self.district_name,
            "state_electoral_division_id": None,
        }


@dataclass(frozen=True)
class Page:
    path: Path
    year: int
    kind: str  # "district" | "region"
    page: str  # "summary" | "distribution" | "fpv" | "tcp"
    slug: str  # slug as it appears in the file name


def classify_page(path: Path, year: int) -> Page | None:
    """Classify one file; ``None`` for the per-election index pages."""
    stem = path.stem.lower()
    if "summary" in stem:
        return None
    stem = stem.removeprefix(f"state{year}")
    if stem.endswith("region"):
        kind, core = "region", stem[: -len("region")]
    elif stem.endswith("district"):
        kind, core = "district", stem[: -len("district")]
    elif stem.endswith("province"):
        kind, core = "region", stem[: -len("province")]
    else:
        return None
    for prefix, family in _PAGE_PREFIXES:
        if core.startswith(prefix):
            return Page(path, year, kind, family, core[len(prefix) :])
    return Page(path, year, kind, "summary", core)


def _contest_name(title: str | None, slug: str) -> str:
    """Prefer the page's own heading; fall back to the file name slug."""
    if title:
        return _TRAILING_CONTEST_WORD.sub("", title).strip()
    return slug.replace("-", " ").title()


# --------------------------------------------------------------------------------------
# Page-level parsers
# --------------------------------------------------------------------------------------


def parse_enrolment(grid: Grid) -> dict[str, object]:
    """Parse the enrolment/turnout box into schema fields.

    Informal and total rows embed a percentage, as in
    ``1575 (4.13% of the total votes)``.
    """
    parsed: dict[str, object] = {
        "enrolment": None,
        "votes_formal": None,
        "votes_informal": None,
        "votes_total": None,
        "percentage_informal": None,
        "percentage_turnout": None,
        "quota": None,
    }
    for row in grid:
        label, raw = _lower(row[0]), row[1]
        match = _NUMBER_WITH_PERCENT.match(raw) if raw else None
        number = _to_int(match.group(1)) if match else None
        percent = (
            _to_percent(match.group(2)) if match and match.group(2) else None
        )
        if "quota" in label:
            parsed["quota"] = number
        elif "informal" in label:
            parsed["votes_informal"] = number
            parsed["percentage_informal"] = percent
        elif "formal" in label:
            parsed["votes_formal"] = number
        elif "enrolment" in label:
            parsed["enrolment"] = number
        elif "total votes" in label:
            parsed["votes_total"] = number
            parsed["percentage_turnout"] = percent
    return parsed


def parse_candidate_results(grid: Grid) -> list[dict[str, object]]:
    """Rows of a Candidate/Party/votes/percentage table, junk row removed."""
    results: list[dict[str, object]] = []
    for row in grid[1:]:
        if _is_repeated_row(row):
            continue
        if row[0] is None:
            continue
        results.append(
            {
                "ballot_name": row[0],
                "party_name": row[1] if len(row) > 1 else None,
                "votes": _to_int(row[2]) if len(row) > 2 else None,
                "percentage": _to_percent(row[3]) if len(row) > 3 else None,
            }
        )
    return results


def parse_elected(text: str, ballot_names: Sequence[str]) -> list[str]:
    """Names of the elected candidates, in the order the page declares them.

    District pages print one name and party; region pages print
    ``1st elected: NAME PARTY 2nd elected: ...``.  Names are matched against the
    contest's own ballot names so that a party name running into the next
    candidate cannot be mis-split.
    """
    if not text:
        return []
    parts = _ELECTED_ORDINAL.split(text)
    segments = parts[1::2] and [
        (int(parts[i]), parts[i + 1]) for i in range(1, len(parts), 2)
    ]
    if not segments:
        segments = [(1, text)]
    elected: list[str] = []
    for _, segment in sorted(segments):
        matched = _match_ballot_name(segment, ballot_names)
        if matched is not None and matched not in elected:
            elected.append(matched)
    return elected


def _match_ballot_name(text: str, ballot_names: Sequence[str]) -> str | None:
    candidates = sorted(ballot_names, key=len, reverse=True)
    stripped = text.strip()
    for name in candidates:
        if stripped.startswith(name):
            return name
    for name in candidates:
        if name in stripped:
            return name
    return None


def parse_distribution(grid: Grid) -> list[dict[str, object]]:
    """Flatten the count-by-candidate distribution matrix into long rows.

    The matrix alternates a transfer row and a ``Progressive Total`` row, ending
    on ``FINAL TOTAL``.  A count therefore reads its transferred votes from its
    own row and its progressive total from the row that follows.  2006 spells
    ``ballot-papers`` where later years spell ``ballot papers``; neither wording
    is matched, only the ``Transfer of`` stem.
    """
    header = grid[0]
    columns = [
        (i, name)
        for i, name in enumerate(header)
        if i > 0 and name is not None and _lower(name) != "total"
    ]
    body = [
        row
        for row in grid[1:]
        if row[0] is not None
        and _ABSOLUTE_MAJORITY_LABEL not in _lower(row[0])
    ]

    counts: list[tuple[str, list[str | None], list[str | None]]] = []
    index = 0
    while index < len(body):
        label = body[index][0] or ""
        lowered = _lower(label)
        if lowered.startswith(_PROGRESSIVE_TOTAL_LABEL) or lowered.startswith(
            _FINAL_TOTAL_LABEL
        ):
            index += 1
            continue
        values = body[index]
        following = body[index + 1] if index + 1 < len(body) else None
        progressive: list[str | None] | None = None
        if following is not None:
            next_label = _lower(following[0])
            if next_label.startswith(
                _PROGRESSIVE_TOTAL_LABEL
            ) or next_label.startswith(_FINAL_TOTAL_LABEL):
                progressive = following
        if lowered.startswith(_FIRST_PREFERENCE_COUNT_LABEL):
            # The opening count is itself the progressive total.
            progressive = values
        counts.append((label, values, progressive or []))
        index += 1

    rows: list[dict[str, object]] = []
    for number, (label, values, progressive) in enumerate(counts, start=1):
        for position, name in columns:
            transferred = _to_int(
                values[position] if position < len(values) else None
            )
            running = _to_int(
                progressive[position] if position < len(progressive) else None
            )
            if transferred is None and running is None:
                continue
            rows.append(
                {
                    "count_number": str(number),
                    "count_description": label,
                    "transfer_value": _ASSEMBLY_TRANSFER_VALUE,
                    "ballot_name": name,
                    "ballot_papers_transferred": transferred,
                    "votes_transferred": transferred,
                    "votes_progressive_total": running,
                }
            )
    return rows


def distribution_final_total(grid: Grid) -> int | None:
    """Sum of the candidate columns on the ``FINAL TOTAL`` row."""
    header = grid[0]
    positions = [
        i
        for i, name in enumerate(header)
        if i > 0 and name is not None and _lower(name) != "total"
    ]
    for row in reversed(grid):
        if _lower(row[0]).startswith(_FINAL_TOTAL_LABEL):
            values = [_to_int(row[i]) for i in positions if i < len(row)]
            present = [v for v in values if v is not None]
            return sum(present) if present else None
    return None


def parse_voting_centres(grid: Grid) -> list[dict[str, object]]:
    """Flatten a candidate-by-voting-centre grid into long rows.

    Row 0 carries ballot names, row 1 carries party names plus the trailing
    ``Mis-sorts``/``Informal votes``/``Total votes polled`` columns, which are
    not candidate votes and are dropped.  Subtotal and total rows are dropped
    too; the declaration-vote rows below the ordinary subtotal become their own
    ``vote_type``.
    """
    names_row, parties_row = grid[0], grid[1]
    reserved = {"mis-sorts", "informal votes", "total votes polled"}
    columns = [
        (i, name, parties_row[i] if i < len(parties_row) else None)
        for i, name in enumerate(names_row)
        if i > 0
        and name is not None
        and _lower(parties_row[i] if i < len(parties_row) else None)
        not in reserved
    ]

    rows: list[dict[str, object]] = []
    seen_ordinary_subtotal = False
    for row in grid[2:]:
        label = row[0]
        if label is None:
            continue
        lowered = _lower(label)
        if lowered.startswith("percentage of formal vote"):
            continue
        if lowered == _ORDINARY_SUBTOTAL_LABEL:
            seen_ordinary_subtotal = True
            continue
        if lowered in _AGGREGATE_ROW_LABELS:
            continue
        if seen_ordinary_subtotal:
            vote_type = _VOTE_TYPE_BY_LABEL.get(lowered)
            if vote_type is None:
                continue
        else:
            vote_type = "ordinary"
        for position, name, party in columns:
            votes = _to_int(row[position] if position < len(row) else None)
            if votes is None:
                continue
            rows.append(
                {
                    "voting_centre_name": label,
                    "vote_type": vote_type,
                    "ballot_name": name,
                    "party_name": party,
                    "votes": votes,
                }
            )
    return rows


def foreign_ballot_names(
    rows: Iterable[Mapping[str, object]], contest_names: Iterable[str]
) -> list[str]:
    """Ballot names on a page that the contest itself never nominated.

    The VEC occasionally republishes a previous election's page under the
    current year's file name — ``state2018/fpvbyvotingcentrebrunswickdistrict``
    is the 2014 page, down to the 2014 candidates and the 2014 formal vote.
    Comparing against the contest's own candidate list catches that class of
    defect without hard-coding the instance.
    """
    known = set(contest_names)
    seen = {str(row["ballot_name"]) for row in rows}
    return sorted(seen - known)


def unmapped_declaration_labels(grid: Grid) -> list[str]:
    """Declaration-row labels below the ordinary subtotal with no vote type."""
    unmapped: list[str] = []
    seen = False
    for row in grid[2:]:
        lowered = _lower(row[0])
        if lowered == _ORDINARY_SUBTOTAL_LABEL:
            seen = True
            continue
        if not seen or not lowered:
            continue
        if lowered.startswith("percentage of formal vote"):
            continue
        if lowered in _AGGREGATE_ROW_LABELS:
            continue
        if lowered not in _VOTE_TYPE_BY_LABEL:
            unmapped.append(row[0] or "")
    return unmapped


# --------------------------------------------------------------------------------------
# Whole-election assembly
# --------------------------------------------------------------------------------------


def _resolve_root(input_root: str | Path) -> Path:
    """Accept either the download root or the ``historical-results`` folder."""
    root = Path(input_root).expanduser()
    if (root / "historical-results").is_dir():
        return root / "historical-results"
    return root


def _split_name(ballot_name: str) -> tuple[str | None, str | None]:
    surname, _, given = ballot_name.partition(",")
    return (surname.strip() or None, given.strip() or None)


def _parse_summary_pages(
    pages: Sequence[Page], report: ParseReport, audit: ParseAudit
) -> tuple[
    dict[str, Contest],
    dict[str, dict[str, str | None]],
    list[dict[str, object]],
    list[dict[str, object]],
    list[dict[str, object]],
]:
    """Contest identity, enrolment, first-preference and TCP district results."""
    contests: dict[str, Contest] = {}
    parties: dict[str, dict[str, str | None]] = {}
    enrolment_rows: list[dict[str, object]] = []
    result_rows: list[dict[str, object]] = []
    candidate_rows: list[dict[str, object]] = []

    for page in pages:
        report.files_attempted += 1
        title, grids = read_page(page.path)
        name = _contest_name(title, page.slug)
        contest = Contest(page.year, page.kind, name)
        key = f"{page.year}:{page.kind}:{page.slug}"
        if title is None:
            report.note("summary page without an h2 heading")

        enrolment_grid = _find_enrolment_grid(grids)
        candidate_grids = _find_candidate_grids(grids)
        if enrolment_grid is None or not candidate_grids:
            report.failures.append(
                (str(page.path), "no enrolment box or no candidate table")
            )
            continue

        contests[key] = contest
        report.contests.add((page.year, page.kind, contest.contest_id))
        report.files_parsed += 1

        block = contest.block()
        totals = parse_enrolment(enrolment_grid)
        enrolment_rows.append(
            {
                **block,
                **totals,
                "seats_to_elect": (
                    SEATS_PER_DISTRICT
                    if contest.is_district
                    else SEATS_PER_REGION
                ),
            }
        )

        first_preference: list[dict[str, object]] = []
        after_distribution: list[dict[str, object]] | None = None
        election_night_preferred: list[dict[str, object]] | None = None
        for header, grid in candidate_grids:
            rows = parse_candidate_results(grid)
            if header.startswith(_FIRST_PREFERENCE_HEADER):
                first_preference = rows
            elif header.startswith(_DISTRIBUTION_HEADER):
                after_distribution = rows
            elif header.startswith(_PREFERRED_HEADER):
                election_night_preferred = rows
            else:
                report.note(f"unrecognised result table header: {header!r}")

        # A contest publishes at most one usable two-candidate-preferred count.
        # The distribution result is definitive; the election-night count stands
        # in only where no distribution was needed.
        if after_distribution is not None:
            two_candidate = after_distribution
            audit.tcp_sources[contest.contest_id] = TCP_FROM_DISTRIBUTION
            report.note(
                "two-candidate-preferred taken from 'Votes after distribution'"
            )
            if election_night_preferred is not None:
                report.note(
                    "'Preferred votes' table present alongside the "
                    "distribution result and not emitted"
                )
        elif election_night_preferred is not None:
            two_candidate = election_night_preferred
            audit.tcp_sources[contest.contest_id] = TCP_FROM_ELECTION_NIGHT
            report.note(
                "two-candidate-preferred taken from 'Preferred votes' "
                "(seat won on first preferences, no distribution published)"
            )
        else:
            two_candidate = []

        emitted = [
            ("first_preference", first_preference),
            ("two_candidate_preferred", two_candidate),
        ]
        for count_type, rows in emitted:
            for row in rows:
                result_rows.append(
                    {
                        **block,
                        "count_type": count_type,
                        "ballot_position": None,
                        # Legislative Assembly districts and Legislative Council
                        # region summaries: neither carries a ballot group here, so
                        # the column is emitted null rather than left absent.
                        "group_letter": None,
                        **row,
                    }
                )

        if not first_preference:
            report.failures.append(
                (str(page.path), "no first-preference table")
            )
            continue

        parties[key] = {
            str(row["ballot_name"]): (
                str(row["party_name"])
                if row["party_name"] is not None
                else None
            )
            for row in first_preference
        }

        ballot_names = [str(row["ballot_name"]) for row in first_preference]
        elected_text = " ".join(
            v for v in (grids[0][0] if grids and grids[0] else []) if v
        )
        elected = parse_elected(elected_text, ballot_names)
        if not elected:
            elected = _elected_from_footer(grids, ballot_names)
        expected = (
            SEATS_PER_DISTRICT if contest.is_district else SEATS_PER_REGION
        )
        if len(elected) != expected:
            report.note(
                f"{expected} elected member(s) expected, "
                f"{len(elected)} matched"
            )

        order = {name: i + 1 for i, name in enumerate(elected)}
        for row in first_preference:
            ballot_name = str(row["ballot_name"])
            surname, given = _split_name(ballot_name)
            candidate_rows.append(
                {
                    **block,
                    "ballot_position": None,
                    "ballot_name": ballot_name,
                    "candidate_surname": surname,
                    "candidate_given_names": given,
                    "party_name": row["party_name"],
                    "group_letter": None,
                    "is_elected": "yes" if ballot_name in order else "no",
                    "elected_order": (
                        None
                        if contest.is_district or ballot_name not in order
                        else str(order[ballot_name])
                    ),
                }
            )

    return contests, parties, enrolment_rows, result_rows, candidate_rows


def _elected_from_footer(
    grids: Sequence[Grid], ballot_names: Sequence[str]
) -> list[str]:
    """2006 pages repeat the winner in an ``Elected member:`` footer row."""
    for grid in grids:
        for row in grid:
            if len(row) == 2 and _lower(row[0]).startswith("elected member"):
                matched = _match_ballot_name(row[1] or "", ballot_names)
                if matched is not None:
                    return [matched]
    return []


def _parse_distribution_pages(
    pages: Sequence[Page],
    contests: Mapping[str, Contest],
    parties: Mapping[str, Mapping[str, str | None]],
    report: ParseReport,
    audit: ParseAudit,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for page in pages:
        report.files_attempted += 1
        key = f"{page.year}:{page.kind}:{page.slug}"
        contest = contests.get(key)
        if contest is None:
            report.failures.append(
                (str(page.path), "no contest page matched this slug")
            )
            continue
        _title, grids = read_page(page.path)
        grid = _find_distribution_grid(grids)
        if grid is None or len(grid) < 2:
            # The VEC published the page with an empty table body.
            report.skipped.append(
                (
                    str(page.path),
                    "source publishes an empty distribution table",
                )
            )
            report.note("distribution page with an empty table body")
            continue
        block = contest.block()
        party_map = parties.get(key, {})
        parsed = parse_distribution(grid)
        foreign = foreign_ballot_names(parsed, party_map)
        if foreign:
            reason = (
                "the page names candidates this contest never nominated "
                f"({', '.join(foreign[:3])}); it is a previous election's "
                "page republished under this year's file name"
            )
            report.skipped.append((str(page.path), reason))
            report.note(
                "distribution page not used: stale page from another election"
            )
            continue
        report.files_parsed += 1
        for row in parsed:
            ballot_name = str(row["ballot_name"])
            rows.append(
                {
                    **block,
                    **row,
                    "party_name": party_map.get(ballot_name),
                }
            )
        total = distribution_final_total(grid)
        if total is not None:
            audit.final_totals[contest.contest_id] = total
    return rows


def _parse_voting_centre_pages(
    pages: Sequence[Page],
    contests: Mapping[str, Contest],
    parties: Mapping[str, Mapping[str, str | None]],
    report: ParseReport,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for page in pages:
        report.files_attempted += 1
        key = f"{page.year}:{page.kind}:{page.slug}"
        contest = contests.get(key)
        if contest is None:
            report.failures.append(
                (str(page.path), "no contest page matched this slug")
            )
            continue
        _title, grids = read_page(page.path)
        grid = _find_voting_centre_grid(grids)
        if grid is None:
            if not any(grids):
                reason = "the source page contains no tables at all"
            elif page.kind == "region":
                reason = (
                    "no candidate-level table; the Legislative Council "
                    "voting-centre pages report party groups, not candidates"
                )
            else:
                reason = "no candidate-by-voting-centre table found"
            report.skipped.append((str(page.path), reason))
            report.note(f"voting-centre page not used: {reason}")
            continue
        for label in unmapped_declaration_labels(grid):
            report.note(f"declaration row dropped as an aggregate: {label!r}")
        block = contest.block()
        party_map = parties.get(key, {})
        count_type = (
            "first_preference"
            if page.page == "fpv"
            else "two_candidate_preferred"
        )
        parsed = parse_voting_centres(grid)
        if not parsed or not any(row["votes"] for row in parsed):
            # Prahran 2014 and Ripon 2018 were recounted centrally: every cell
            # of the breakdown is zero and the district's whole vote sits on a
            # single "All Votes" line.  Publishing the zeros would assert that
            # nobody voted at those centres, which is false.
            reason = (
                "every voting centre reports zero votes; the recount is "
                "published only as a district-wide 'All Votes' line"
            )
            report.skipped.append((str(page.path), reason))
            report.note(f"voting-centre page not used: {reason}")
            continue
        foreign = foreign_ballot_names(parsed, party_map)
        if foreign:
            reason = (
                "the page names candidates this contest never nominated "
                f"({', '.join(foreign[:3])}); it is a previous election's "
                "page republished under this year's file name"
            )
            report.skipped.append((str(page.path), reason))
            report.note(
                "voting-centre page not used: stale page from another election"
            )
            continue
        report.files_parsed += 1
        for row in parsed:
            ballot_name = str(row["ballot_name"])
            rows.append(
                {
                    **block,
                    "voting_centre_name": row["voting_centre_name"],
                    "vote_type": row["vote_type"],
                    "count_type": count_type,
                    "ballot_position": None,
                    "ballot_name": ballot_name,
                    "party_name": row["party_name"]
                    or party_map.get(ballot_name),
                    # Only Legislative Assembly districts publish voting-centre pages
                    # in this era, so there is no group to record.
                    "group_letter": None,
                    "votes": row["votes"],
                }
            )
    return rows


def _frame(table: str, rows: Sequence[Mapping[str, object]]) -> pd.DataFrame:
    columns = schema.column_names(table)
    frame = pd.DataFrame(list(rows), columns=columns)
    if not frame.empty:
        frame["year"] = frame["year"].astype("int64")
    return frame


def parse_all_with_report(
    input_root: str | Path,
) -> tuple[dict[str, pd.DataFrame], ParseReport, ParseAudit]:
    """Parse every HTML year and return the tables, a report and an audit."""
    root = _resolve_root(input_root)
    report = ParseReport()
    audit = ParseAudit()

    enrolment_rows: list[dict[str, object]] = []
    result_rows: list[dict[str, object]] = []
    centre_rows: list[dict[str, object]] = []
    distribution_rows: list[dict[str, object]] = []
    candidate_rows: list[dict[str, object]] = []

    for year in HTML_YEARS:
        year_dir = root / f"state{year}"
        if not year_dir.is_dir():
            report.failures.append((str(year_dir), "missing year directory"))
            continue
        pages: list[Page] = []
        for path in sorted(year_dir.glob("*.html")):
            page = classify_page(path, year)
            if page is None:
                report.skipped.append((str(path), "index page, no data"))
                continue
            pages.append(page)

        by_family: dict[str, list[Page]] = {}
        for page in pages:
            by_family.setdefault(page.page, []).append(page)

        print(
            f"[{year}] {len(pages)} pages: "
            + ", ".join(
                f"{family}={len(items)}"
                for family, items in sorted(by_family.items())
            ),
            flush=True,
        )

        (
            contests,
            parties,
            year_enrolment,
            year_results,
            year_candidates,
        ) = _parse_summary_pages(by_family.get("summary", []), report, audit)
        print(f"[{year}] contest pages parsed: {len(contests)}", flush=True)

        year_distribution = _parse_distribution_pages(
            by_family.get("distribution", []), contests, parties, report, audit
        )
        print(
            f"[{year}] distribution rows: {len(year_distribution)}", flush=True
        )

        year_centres = _parse_voting_centre_pages(
            by_family.get("fpv", []) + by_family.get("tcp", []),
            contests,
            parties,
            report,
        )
        print(f"[{year}] voting-centre rows: {len(year_centres)}", flush=True)

        enrolment_rows += year_enrolment
        result_rows += year_results
        candidate_rows += year_candidates
        distribution_rows += year_distribution
        centre_rows += year_centres

    tables = {
        "enrolment_turnout": _frame("enrolment_turnout", enrolment_rows),
        "result_district": _frame("result_district", result_rows),
        "result_voting_centre": _frame("result_voting_centre", centre_rows),
        "distribution_of_preferences": _frame(
            "distribution_of_preferences", distribution_rows
        ),
        "candidate": _frame("candidate", candidate_rows),
    }
    return tables, report, audit


def parse_all(input_root: str) -> dict[str, pd.DataFrame]:
    """Parse the 2006-2018 HTML pages into one frame per table.

    Raises :class:`ParseError` if any page that was expected to yield data did
    not, so that a parse failure can never become a silently dropped file.
    """
    tables, report, _ = parse_all_with_report(input_root)
    if report.failures:
        listed = "\n".join(f"  {path}: {why}" for path, why in report.failures)
        raise ParseError(
            f"{len(report.failures)} page(s) failed to parse:\n{listed}"
        )
    return tables


# --------------------------------------------------------------------------------------
# Internal consistency
# --------------------------------------------------------------------------------------

CHECK_FIRST_PREFERENCE = "first_preference_sums_to_formal"
CHECK_TWO_CANDIDATE = "two_candidate_preferred_sums_to_formal"
CHECK_DISTRIBUTION = "distribution_final_total_equals_formal"


def check_consistency(
    tables: Mapping[str, pd.DataFrame],
    audit: ParseAudit | None = None,
) -> pd.DataFrame:
    """Compare every contest's counts against its formal vote.

    Returns one row per (contest, check) with ``passed``, the two values and the
    source the value came from, so that failures can be counted per year and
    inspected individually rather than passing silently.  The ``source`` column
    matters for the two-candidate-preferred check: a count taken from
    ``Preferred votes`` is the election-night count and is expected to differ
    from the formal vote by the mis-sorts, whereas one taken from the
    distribution is expected to match exactly.
    """
    final_totals = audit.final_totals if audit else {}
    tcp_sources = audit.tcp_sources if audit else {}
    enrolment = tables["enrolment_turnout"]
    results = tables["result_district"]
    formal = {
        str(row.contest_id): row.votes_formal
        for row in enrolment.itertuples(index=False)
    }
    years = {
        str(row.contest_id): int(row.year)
        for row in enrolment.itertuples(index=False)
    }

    sums = (
        results.groupby(["contest_id", "count_type"], dropna=False)["votes"]
        .sum()
        .to_dict()
    )

    rows: list[dict[str, object]] = []

    def add(
        contest_id: str, check: str, actual: object, source: str = ""
    ) -> None:
        expected = formal.get(contest_id)
        rows.append(
            {
                "year": years.get(contest_id),
                "contest_id": contest_id,
                "check": check,
                "source": source,
                "expected": expected,
                "actual": actual,
                "difference": (
                    None
                    if actual is None or expected is None
                    else int(actual) - int(expected)
                ),
                "passed": actual is not None
                and expected is not None
                and int(actual) == int(expected),
            }
        )

    for contest_id in formal:
        add(
            contest_id,
            CHECK_FIRST_PREFERENCE,
            sums.get((contest_id, "first_preference")),
        )
        tcp = sums.get((contest_id, "two_candidate_preferred"))
        if tcp is not None:
            add(
                contest_id,
                CHECK_TWO_CANDIDATE,
                tcp,
                tcp_sources.get(contest_id, ""),
            )
        if contest_id in final_totals:
            add(
                contest_id,
                CHECK_DISTRIBUTION,
                final_totals[contest_id],
                TCP_FROM_DISTRIBUTION,
            )

    return pd.DataFrame(
        rows,
        columns=[
            "year",
            "contest_id",
            "check",
            "source",
            "expected",
            "actual",
            "difference",
            "passed",
        ],
    )
