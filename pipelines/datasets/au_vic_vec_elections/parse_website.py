"""Scrape the Legislative Assembly counts the VEC publishes only as web pages.

The 2022 state election's **two-candidate-preferred** result, its **distribution of
preferences**, and its **2CP by voting centre** breakdown are not in the Azure blob
container that supplies the rest of this dataset. They are server-rendered on
``www.vec.vic.gov.au``. The same is true of every by-election since 2023 and of the
Narracan supplementary election. This module is the only source for those counts.

Four properties of the source drive the design:

1. **Do not construct URLs — scrape them.** Ten districts have no distribution page at
   all (they were won on first preferences), and Pakenham's has a double dash
   (``pakenham-results--distribution``; the single-dash form 404s). By-election slugs
   capitalise the district (``Nepean-results-distribution``) while 2022's are lowercase.
   Every link is therefore taken from the district page's own anchors.

2. **Classify a table by the heading above it, not by its columns.** ``Two candidate
   preferred vote`` and ``Two party preferred vote`` are two different counts of the same
   contest, and the column order is not a reliable discriminator: Warrandyte's 2023
   by-election renders its *two-candidate* table with the party column first, exactly
   like every two-party table.

3. **Never use the election-night blob.** ``stpublishedresultwsprd01.blob.core.windows.net``
   serves rich JSON for 2022 and looks authoritative, but it is a frozen partial count
   (``CountProgress: 68.46``): Albert Park's TCP there is 9,979 against a final 23,916.

4. **The indicative distributions are not legal counts.** The VEC conducted full
   preference distributions for 38 of the 2022 districts, for Narracan and for the
   Warrandyte by-election in 2023, "for information and statistical purposes only". They
   are returned under their own key, never merged with the on-page legal distributions.

The boilerplate "This district would normally not require a full preference
distribution" is present on both absolute-majority districts and districts whose count
stopped with more than two candidates remaining, so it classifies nothing and is ignored.
"""

from __future__ import annotations

import datetime as dt
import re
import time
import unicodedata
from dataclasses import dataclass
from html import unescape
from pathlib import Path
from typing import Final

# pyrefly: ignore [untyped-import]
import openpyxl
import pandas as pd
import requests

# pyrefly: ignore [untyped-import]
import xlrd

from pipelines.datasets.au_vic_vec_elections import schema

# --------------------------------------------------------------------------------------
# Source constants
# --------------------------------------------------------------------------------------

SITE: Final[str] = "https://www.vec.vic.gov.au"
HUB_2022_URL: Final[str] = (
    SITE + "/results/state-election-results/2022-state-election-results"
)
BY_ELECTION_TIMELINE_URL: Final[str] = (
    SITE + "/results/state-election-results/state-by-elections-timeline"
)
INDICATIVE_URL: Final[str] = (
    SITE
    + "/results/electoral-statistics/state-election-statistics/full-preference-distributions"
)

USER_AGENT: Final[str] = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"
)
REQUEST_TIMEOUT: Final[int] = 120
#: Politeness delay between live requests, in seconds.
REQUEST_DELAY: Final[float] = 0.35

ELECTION_2022_ID: Final[str] = "state2022"
ELECTION_2022_NAME: Final[str] = "State Election 2022"
ELECTION_2022_DATE: Final[dt.date] = dt.date(2022, 11, 26)

CHAMBER: Final[str] = "legislative_assembly"
CONTEST_TYPE: Final[str] = "state_district"
GOVERNMENT_LEVEL: Final[str] = "state"
VOTING_SYSTEM: Final[str] = "compulsory_preferential"

COUNT_TCP: Final[str] = "two_candidate_preferred"
COUNT_TPP: Final[str] = "two_party_preferred"

#: The Narracan general election of 2022 was voided; the seat was filled by a
#: supplementary election on 28 January 2023. Its page carries no year, so the id is
#: pinned here rather than derived from the poll date.
ELECTION_ID_BY_PAGE: Final[dict[str, str]] = {
    # Narracan's 2022 general-election contest was voided when a candidate died
    # during the campaign; the seat was filled at a supplementary election held on
    # 28 January 2023. It is a 2023 event, so the id carries 2023, not 2022.
    "narracan-district-supplementary-election-results": "narracan_supp2023",
}

# Headings that introduce a table on a district page.
HEADING_FIRST_PREFERENCE: Final[str] = "Recheck first preference votes"
HEADING_AFTER_DISTRIBUTION: Final[str] = (
    "Results after distribution of preferences"
)
HEADING_TWO_CANDIDATE: Final[str] = "Two candidate preferred vote"
HEADING_TWO_PARTY: Final[str] = "Two party preferred vote"

#: Row labels on a "2CP by voting centre" table that are not voting centres.
VOTING_CENTRE_SUBTOTALS: Final[frozenset[str]] = frozenset(
    {
        "Ordinary votes total",
        "Total",
        "Percentage of formal vote polled by candidate",
        "Voting centres",
    }
)
#: Declaration-vote rows, reported once for the whole contest.
DECLARATION_VOTE_TYPES: Final[dict[str, str]] = {
    "Absent votes": "absent",
    "Early votes": "early",
    "Postal votes": "postal",
    "Provisional votes": "provisional",
    "Marked As Voted votes": "marked_as_voted",
}
#: An empty declaration bucket the CMS emits in 12 districts, always all-zero. It is not
#: one of the six vote types the schema admits, so it is dropped rather than invented.
VOTING_CENTRE_EMPTY_BUCKET: Final[str] = "All Votes votes"

VOTE_TYPE_ORDINARY: Final[str] = "ordinary"

#: Distribution-of-preferences row labels. Case is not stable across the estate: 2022
#: renders ``FINAL TOTAL`` and a ``TOTAL`` column, the by-election pages render
#: ``Final Total`` and ``Total``. Every comparison below is therefore case-folded — the
#: title-case variant would otherwise be read as a ninth candidate whose running total
#: never resolves.
DOP_FIRST_PREFERENCE_PREFIX: Final[str] = "total first preference votes"
DOP_TRANSFER_PREFIX: Final[str] = "transfer of "
DOP_PROGRESSIVE_LABELS: Final[frozenset[str]] = frozenset(
    {"progressive total", "final total"}
)
DOP_CANDIDATE_HEADER_PREFIX: Final[str] = "candidates names"
DOP_TOTAL_COLUMN: Final[str] = "total"

#: Every Legislative Assembly ballot paper is transferred at full value; fractional
#: transfer values occur only in the Legislative Council's single-transferable-vote count.
LA_TRANSFER_VALUE: Final[float] = 1.0

MONTHS: Final[dict[str, int]] = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
}

TABLE_ELECTION: Final[str] = "election"
TABLE_RESULT_DISTRICT: Final[str] = "result_district"
TABLE_RESULT_VOTING_CENTRE: Final[str] = "result_voting_centre"
TABLE_DOP: Final[str] = "distribution_of_preferences"
KEY_DOP_INDICATIVE: Final[str] = "distribution_of_preferences_indicative"

# --------------------------------------------------------------------------------------
# Regular expressions
# --------------------------------------------------------------------------------------

_SCRIPT_RE: Final[re.Pattern[str]] = re.compile(
    r"(?s)<(script|style|noscript)\b.*?</\1>"
)
_MAIN_RE: Final[re.Pattern[str]] = re.compile(r"(?s)<main\b.*?</main>")
_BLOCK_RE: Final[re.Pattern[str]] = re.compile(
    r"(?s)(?P<heading><h[1-6]\b[^>]*>.*?</h[1-6]>)|(?P<table><table\b.*?</table>)"
)
_ROW_RE: Final[re.Pattern[str]] = re.compile(r"(?s)<tr\b.*?</tr>")
_CELL_RE: Final[re.Pattern[str]] = re.compile(
    r"(?s)<t[hd]\b[^>]*>(.*?)</t[hd]>"
)
_TAG_RE: Final[re.Pattern[str]] = re.compile(r"(?s)<[^>]+>")
_ANCHOR_RE: Final[re.Pattern[str]] = re.compile(
    r"(?s)<a\b[^>]*href=\"([^\"]+)\"[^>]*>"
)
_ACCORDION_YEAR_RE: Final[re.Pattern[str]] = re.compile(
    r"accordion-item\"[^>]*>\s*(\d{4})\s*</a>"
)
_MEDIA_LINK_RE: Final[re.Pattern[str]] = re.compile(
    r"(?s)<a\b[^>]*href=\"(/-/media/[^\"]+\.xlsx?)\"[^>]*>(.*?)</a>",
    re.IGNORECASE,
)
_DISTRICT_LINK_RE: Final[re.Pattern[str]] = re.compile(
    r"href=\"(/results/state-election-results/2022-state-election-results"
    r"/results-by-district/[a-z0-9\-]+-district-results)\""
)
_DOP_HREF_RE: Final[re.Pattern[str]] = re.compile(
    r"href=\"([^\"]*results-+distribution[^\"]*)\"", re.IGNORECASE
)
_VC_HREF_RE: Final[re.Pattern[str]] = re.compile(
    r"href=\"([^\"]*2cp-results-by-voting-centre[^\"]*)\"", re.IGNORECASE
)
_FORMAL_VOTES_RE: Final[re.Pattern[str]] = re.compile(
    r"Formal votes:?\s*([\d,]+)", re.IGNORECASE
)
_DISTRICT_NAME_RE: Final[re.Pattern[str]] = re.compile(r"^(.*?)\s+District\b")
_DAY_MONTH_RE: Final[re.Pattern[str]] = re.compile(
    r"(\d{1,2})\s+([A-Za-z]+)", re.IGNORECASE
)
_INDICATIVE_LABEL_RE: Final[re.Pattern[str]] = re.compile(
    r"^(?P<name>.+?)\s*-\s*indicative distribution of preference",
    re.IGNORECASE,
)
_NON_ALNUM_RE: Final[re.Pattern[str]] = re.compile(r"[^a-z0-9]+")


# --------------------------------------------------------------------------------------
# Small pure helpers
# --------------------------------------------------------------------------------------


def _squash(text: str) -> str:
    """Collapse all whitespace runs to single spaces and strip the ends."""
    return re.sub(r"\s+", " ", text).strip()


def _text_of(fragment: str) -> str:
    """Strip tags from an HTML fragment and normalise its whitespace."""
    return _squash(unescape(_TAG_RE.sub(" ", fragment)))


def _slugify(name: str) -> str:
    """Lowercase, drop accents, collapse every non-alphanumeric run to a single ``_``."""
    folded = unicodedata.normalize("NFKD", name)
    folded = "".join(ch for ch in folded if not unicodedata.combining(ch))
    return _NON_ALNUM_RE.sub("_", folded.lower()).strip("_")


def _clean_value(raw: str | None) -> str:
    """Normalise a source cell: blanks, ``-`` and ``N/A`` all become the empty string."""
    if raw is None:
        return ""
    text = _squash(unescape(raw))
    if text in {"", "-", "\u2013", "\u2014", "N/A", "n/a", "nan", "NaN"}:
        return ""
    return text


def _to_int(raw: str | None) -> int | None:
    """Parse a vote count. Blank, ``-`` and unparseable values are NULL, never zero."""
    text = (
        _clean_value(raw)
        .replace(",", "")
        .replace(" ", "")
        .replace("\u00a0", "")
    )
    if not text:
        return None
    if text.endswith(".0"):
        text = text[:-2]
    try:
        return int(text)
    except ValueError:
        return None


def _to_percent(raw: str | None) -> float | None:
    """Parse a percentage stored 0-100. Blank and unparseable values are NULL."""
    text = _clean_value(raw).replace(",", "").rstrip("%").strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _absolute(href: str) -> str:
    """Resolve a site-relative href against the VEC site root."""
    href = unescape(href).strip()
    if href.startswith("http://") or href.startswith("https://"):
        return href
    return SITE + href


def _district_name_from_title(title: str) -> str:
    """``"Albert Park District results"`` -> ``"Albert Park"``."""
    match = _DISTRICT_NAME_RE.match(title)
    if match is None:
        raise ValueError(f"page title does not name a district: {title!r}")
    return match.group(1).strip()


# --------------------------------------------------------------------------------------
# HTML structure
# --------------------------------------------------------------------------------------


@dataclass(frozen=True)
class Section:
    """One ``<table>`` together with the heading that introduces it."""

    heading: str
    rows: tuple[tuple[str, ...], ...]

    @property
    def header(self) -> tuple[str, ...]:
        return self.rows[0] if self.rows else ()

    @property
    def body(self) -> tuple[tuple[str, ...], ...]:
        return self.rows[1:]


def _strip_chrome(document: str) -> str:
    """Remove scripts and styles, then narrow to ``<main>`` when the page has one."""
    body = _SCRIPT_RE.sub(" ", document)
    match = _MAIN_RE.search(body)
    return match.group(0) if match else body


def _table_rows(table_html: str) -> tuple[tuple[str, ...], ...]:
    return tuple(
        tuple(_clean_value(cell) for cell in _CELL_RE.findall(row))
        for row in _ROW_RE.findall(table_html)
    )


def _sections(document: str) -> list[Section]:
    """Every table on the page, each tagged with the nearest preceding heading."""
    content = _strip_chrome(document)
    heading = ""
    found: list[Section] = []
    for match in _BLOCK_RE.finditer(content):
        if match.group("heading") is not None:
            text = _text_of(match.group("heading"))
            if text:
                heading = text
            continue
        found.append(
            Section(heading=heading, rows=_table_rows(match.group("table")))
        )
    return found


def _page_title(document: str) -> str:
    content = _strip_chrome(document)
    for match in _BLOCK_RE.finditer(content):
        fragment = match.group("heading")
        if fragment is not None and fragment.lower().startswith("<h1"):
            return _text_of(fragment)
    raise ValueError("page has no <h1>")


def _hrefs(document: str, pattern: re.Pattern[str]) -> list[str]:
    """Distinct hrefs matching ``pattern``, in document order."""
    seen: list[str] = []
    for href in pattern.findall(_strip_chrome(document)):
        resolved = unescape(href)
        if resolved not in seen:
            seen.append(resolved)
    return seen


def _named_rows(section: Section) -> list[dict[str, str]]:
    """Body rows of a headed table keyed by their column names."""
    header = section.header
    return [
        dict(zip(header, row, strict=False))
        for row in section.body
        if any(row)
    ]


def _pick_column(header: tuple[str, ...], *, wanted: str) -> str:
    """Find the one header cell containing ``wanted`` and not being a percentage."""
    for name in header:
        lowered = name.lower()
        if wanted in lowered and not lowered.startswith("%"):
            return name
    raise KeyError(f"no {wanted!r} column in {header!r}")


def _percent_column(header: tuple[str, ...]) -> str:
    for name in header:
        if name.startswith("%"):
            return name
    raise KeyError(f"no percentage column in {header!r}")


# --------------------------------------------------------------------------------------
# Fetching
# --------------------------------------------------------------------------------------


class Fetcher:
    """Caching HTTP client. Every response is written to disk and reused thereafter."""

    def __init__(self, cache_dir: Path, *, refresh: bool = False) -> None:
        self.cache_dir = cache_dir
        self.refresh = refresh
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})
        self.attempted: int = 0
        self.downloaded: int = 0
        self.failures: list[tuple[str, str]] = []

    def _sleep(self) -> None:
        time.sleep(REQUEST_DELAY)

    def get_text(self, url: str, name: str) -> str | None:
        """Return the page body, or ``None`` when the request failed."""
        path = self.cache_dir / name
        self.attempted += 1
        if path.exists() and path.stat().st_size > 0 and not self.refresh:
            return path.read_text(encoding="utf-8")
        try:
            response = self.session.get(url, timeout=REQUEST_TIMEOUT)
        except requests.RequestException as error:
            self.failures.append((url, f"request error: {error}"))
            return None
        finally:
            self._sleep()
        if response.status_code != 200:
            self.failures.append((url, f"HTTP {response.status_code}"))
            return None
        response.encoding = response.encoding or "utf-8"
        path.write_text(response.text, encoding="utf-8")
        self.downloaded += 1
        return response.text

    def get_binary(self, url: str, name: str) -> Path | None:
        """Download a workbook to the cache and return its path, or ``None`` on failure."""
        path = self.cache_dir / name
        self.attempted += 1
        if path.exists() and path.stat().st_size > 0 and not self.refresh:
            return path
        try:
            response = self.session.get(url, timeout=REQUEST_TIMEOUT)
        except requests.RequestException as error:
            self.failures.append((url, f"request error: {error}"))
            return None
        finally:
            self._sleep()
        if response.status_code != 200:
            self.failures.append((url, f"HTTP {response.status_code}"))
            return None
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(response.content)
        self.downloaded += 1
        return path


# --------------------------------------------------------------------------------------
# Electoral events
# --------------------------------------------------------------------------------------


@dataclass(frozen=True)
class Event:
    """One electoral event: the 2022 general election or a single by-election."""

    election_id: str
    election_name: str
    election_type: str
    election_date: dt.date
    source_url: str

    @property
    def year(self) -> int:
        return self.election_date.year


def _parse_timeline_date(text: str, year: int) -> dt.date:
    """``"Saturday 2 May"`` with year 2026 -> ``date(2026, 5, 2)``."""
    match = _DAY_MONTH_RE.search(text)
    if match is None:
        raise ValueError(f"no day/month in timeline cell {text!r}")
    month = MONTHS.get(match.group(2).lower())
    if month is None:
        raise ValueError(f"unknown month in timeline cell {text!r}")
    return dt.date(year, month, int(match.group(1)))


def _by_election_events(document: str) -> list[tuple[Event, str]]:
    """Parse the by-election timeline into events paired with their results-page href.

    Only events with a dedicated results page are returned. Elections from 2017 and
    earlier are rendered through a ``?year=&target=`` query page with a different
    structure, and are out of scope here.
    """
    content = _strip_chrome(document)
    boundaries = list(_ACCORDION_YEAR_RE.finditer(content))
    events: list[tuple[Event, str]] = []
    for index, marker in enumerate(boundaries):
        year = int(marker.group(1))
        end = (
            boundaries[index + 1].start()
            if index + 1 < len(boundaries)
            else len(content)
        )
        block = content[marker.end() : end]
        for row_html in _ROW_RE.findall(block):
            cells = _CELL_RE.findall(row_html)
            if len(cells) < 2:
                continue
            date_text = _clean_value(cells[0])
            label = _text_of(cells[1])
            anchor = _ANCHOR_RE.search(cells[1])
            if anchor is None or not date_text or date_text.lower() == "date":
                continue
            href = unescape(anchor.group(1))
            if "?" in href:
                continue
            page_key = href.rstrip("/").rsplit("/", 1)[-1]
            district = _district_name_from_title(label)
            election_id = ELECTION_ID_BY_PAGE.get(
                page_key, f"{_slugify(district)}_by{year}"
            )
            events.append(
                (
                    Event(
                        election_id=election_id,
                        election_name=label,
                        election_type="state_by_election",
                        election_date=_parse_timeline_date(date_text, year),
                        source_url=_absolute(href),
                    ),
                    href,
                )
            )
    return events


# --------------------------------------------------------------------------------------
# Contest pages
# --------------------------------------------------------------------------------------


@dataclass(frozen=True)
class Contest:
    """A single Legislative Assembly seat contest and the pages that describe it."""

    event: Event
    district_name: str
    page_url: str
    formal_votes: int | None
    #: candidate -> party, from the recheck first-preference table
    party_of: dict[str, str]
    #: candidate -> 1-based ballot-paper position
    ballot_position_of: dict[str, str]
    #: (count_type, rows) for the two-candidate and two-party tables present on the page
    result_sections: tuple[tuple[str, Section], ...]
    distribution_href: str | None
    voting_centre_href: str | None

    @property
    def contest_id(self) -> str:
        return f"{self.event.election_id}-{_slugify(self.district_name)}"

    def contest_block(self) -> dict[str, object]:
        """The nine columns spliced into every contest-level fact table."""
        return {
            "year": self.event.year,
            "election_id": self.event.election_id,
            "contest_id": self.contest_id,
            "chamber": CHAMBER,
            "government_level": GOVERNMENT_LEVEL,
            "contest_type": CONTEST_TYPE,
            "voting_system": VOTING_SYSTEM,
            "district_name": self.district_name,
            "state_electoral_division_id": None,
        }


def _parse_contest_page(document: str, event: Event, page_url: str) -> Contest:
    sections = _sections(document)
    title = _page_title(document)
    district_name = _district_name_from_title(title)

    formal_match = _FORMAL_VOTES_RE.search(_text_of(_strip_chrome(document)))
    formal_votes = _to_int(formal_match.group(1)) if formal_match else None

    party_of: dict[str, str] = {}
    ballot_position_of: dict[str, str] = {}
    results: list[tuple[str, Section]] = []
    for section in sections:
        if not section.rows:
            continue
        if section.heading == HEADING_FIRST_PREFERENCE:
            candidate_col = _pick_column(section.header, wanted="candidate")
            party_col = _pick_column(section.header, wanted="party")
            for position, row in enumerate(_named_rows(section), start=1):
                name = row.get(candidate_col, "")
                if not name:
                    continue
                party_of[name] = row.get(party_col, "")
                ballot_position_of[name] = str(position)
        elif section.heading in (
            HEADING_AFTER_DISTRIBUTION,
            HEADING_TWO_CANDIDATE,
        ):
            results.append((COUNT_TCP, section))
        elif section.heading == HEADING_TWO_PARTY:
            results.append((COUNT_TPP, section))

    distributions = [
        href
        for href in _hrefs(document, _DOP_HREF_RE)
        if "full-preference-distribution" not in href.lower()
    ]
    voting_centres = _hrefs(document, _VC_HREF_RE)

    return Contest(
        event=event,
        district_name=district_name,
        page_url=page_url,
        formal_votes=formal_votes,
        party_of=party_of,
        ballot_position_of=ballot_position_of,
        result_sections=tuple(results),
        distribution_href=distributions[0] if distributions else None,
        voting_centre_href=voting_centres[0] if voting_centres else None,
    )


def _result_district_records(contest: Contest) -> list[dict[str, object]]:
    """TCP and, where a separate table exists, two-party-preferred rows.

    ``group_letter`` is emitted null throughout: this module covers Legislative
    Assembly districts only, and a district ballot has no groups.
    """
    block = contest.contest_block()
    records: list[dict[str, object]] = []
    for count_type, section in contest.result_sections:
        candidate_col = _pick_column(section.header, wanted="candidate")
        party_col = _pick_column(section.header, wanted="party")
        votes_col = _pick_column(section.header, wanted="votes")
        percent_col = _percent_column(section.header)
        for row in _named_rows(section):
            name = row.get(candidate_col, "")
            if not name:
                continue
            party = row.get(party_col, "") or contest.party_of.get(name, "")
            records.append(
                {
                    **block,
                    "count_type": count_type,
                    "ballot_position": contest.ballot_position_of.get(name),
                    "ballot_name": name,
                    "party_name": party or None,
                    "group_letter": None,
                    "votes": _to_int(row.get(votes_col)),
                    "percentage": _to_percent(row.get(percent_col)),
                }
            )
    return records


# --------------------------------------------------------------------------------------
# 2CP by voting centre
# --------------------------------------------------------------------------------------


def _voting_centre_records(
    contest: Contest, document: str
) -> tuple[list[dict[str, object]], list[str]]:
    """Rows from a "2CP results by voting centre" page, plus any labels it dropped.

    ``group_letter`` is emitted null throughout, for the same reason as in
    :func:`_result_district_records`: these are Legislative Assembly districts.
    """
    sections = _sections(document)
    tables = [section for section in sections if len(section.rows) > 2]
    if not tables:
        return [], []
    rows = tables[0].rows

    # Row 0 carries the two candidate names; row 1 carries their parties and the labels
    # of the non-candidate columns (mis-sorts, informal, total).
    names = rows[0]
    parties = rows[1] if len(rows) > 1 else ()
    columns = [index for index in range(1, len(names)) if names[index]]
    if not columns:
        return [], []

    block = contest.contest_block()
    records: list[dict[str, object]] = []
    dropped: list[str] = []
    for row in rows[2:]:
        if not row or not row[0]:
            continue
        label = row[0]
        if label in VOTING_CENTRE_SUBTOTALS:
            continue
        if label == VOTING_CENTRE_EMPTY_BUCKET:
            dropped.append(label)
            continue
        vote_type = DECLARATION_VOTE_TYPES.get(label)
        if vote_type is None:
            vote_type = VOTE_TYPE_ORDINARY
            centre_name = label
        else:
            centre_name = vote_type
        for index in columns:
            if index >= len(row):
                continue
            name = names[index]
            party = parties[index] if index < len(parties) else ""
            records.append(
                {
                    **block,
                    "voting_centre_name": centre_name,
                    "vote_type": vote_type,
                    "count_type": COUNT_TCP,
                    "ballot_position": contest.ballot_position_of.get(name),
                    "ballot_name": name,
                    "party_name": (party or contest.party_of.get(name, ""))
                    or None,
                    "group_letter": None,
                    "votes": _to_int(row[index]),
                }
            )
    return records, dropped


# --------------------------------------------------------------------------------------
# Distribution of preferences
# --------------------------------------------------------------------------------------


def _distribution_records(
    contest: Contest,
    header: tuple[str, ...],
    body: tuple[tuple[str, ...], ...],
) -> list[dict[str, object]]:
    """Turn a wide distribution table (candidates across, counts down) into long rows.

    The first body row is the first-preference tally; each later count is a
    ``Transfer of N ballot papers of X`` row immediately followed by a
    ``Progressive Total`` (or, for the last count, ``FINAL TOTAL``) row. A blank cell
    means the candidate had already been excluded, and is emitted as NULL rather than 0.
    """
    candidates = {
        index: name
        for index, name in enumerate(header)
        if index > 0 and name and name.casefold() != DOP_TOTAL_COLUMN
    }
    block = contest.contest_block()
    records: list[dict[str, object]] = []
    count_number = 0
    pending: tuple[int, str, tuple[str, ...]] | None = None

    def emit(
        number: int,
        description: str,
        transferred: tuple[str, ...],
        progressive: tuple[str, ...],
    ) -> None:
        for index, name in candidates.items():
            moved = (
                _to_int(transferred[index])
                if index < len(transferred)
                else None
            )
            total = (
                _to_int(progressive[index])
                if index < len(progressive)
                else None
            )
            if moved is None and total is None:
                continue
            records.append(
                {
                    **block,
                    "count_number": str(number),
                    "count_description": description,
                    "transfer_value": LA_TRANSFER_VALUE,
                    "ballot_name": name,
                    "party_name": contest.party_of.get(name) or None,
                    "ballot_papers_transferred": moved,
                    "votes_transferred": moved,
                    "votes_progressive_total": total,
                }
            )

    for row in body:
        if not row or not row[0]:
            continue
        label = row[0]
        folded = label.casefold()
        if folded.startswith(DOP_FIRST_PREFERENCE_PREFIX):
            count_number += 1
            emit(count_number, label, row, row)
        elif folded.startswith(DOP_TRANSFER_PREFIX):
            if pending is not None:
                number, description, transferred = pending
                emit(number, description, transferred, ())
            count_number += 1
            pending = (count_number, label, row)
        elif folded in DOP_PROGRESSIVE_LABELS and pending is not None:
            number, description, transferred = pending
            emit(number, description, transferred, row)
            pending = None
    if pending is not None:
        number, description, transferred = pending
        emit(number, description, transferred, ())
    return records


def _distribution_from_page(
    contest: Contest, document: str
) -> list[dict[str, object]]:
    sections = _sections(document)
    tables = [section for section in sections if len(section.rows) > 1]
    if not tables:
        return []
    rows = tables[0].rows
    return _distribution_records(contest, rows[0], rows[1:])


# --------------------------------------------------------------------------------------
# Indicative distributions (2023 statistical counts)
# --------------------------------------------------------------------------------------


def _cell_text(value: object) -> str:
    """Normalise one workbook cell to the same shape as an HTML cell."""
    if value is None:
        return ""
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return _squash(str(value))


def _workbook_rows(path: Path) -> list[tuple[str, ...]]:
    if path.suffix.lower() == ".xlsx":
        book = openpyxl.load_workbook(path, data_only=True, read_only=True)
        try:
            sheet = book.worksheets[0]
            return [
                tuple(_cell_text(cell) for cell in row)
                for row in sheet.iter_rows(values_only=True)
            ]
        finally:
            book.close()
    book_xls = xlrd.open_workbook(str(path))
    sheet_xls = book_xls.sheet_by_index(0)
    return [
        tuple(_cell_text(cell.value) for cell in sheet_xls.row(index))
        for index in range(sheet_xls.nrows)
    ]


def _indicative_target(label: str) -> tuple[str, str] | None:
    """Map an indicative-distribution link label to ``(kind, district name)``.

    ``kind`` is ``"general"``, ``"by_election"`` or ``"supplementary"``.
    """
    match = _INDICATIVE_LABEL_RE.match(label)
    if match is None:
        return None
    name = match.group("name").strip()
    if name.endswith("District by-election"):
        return "by_election", name[: -len("District by-election")].strip()
    if name.endswith("District supplementary election"):
        return "supplementary", name[
            : -len("District supplementary election")
        ].strip()
    if name.endswith("District"):
        return "general", name[: -len("District")].strip()
    return None


# --------------------------------------------------------------------------------------
# Frame assembly
# --------------------------------------------------------------------------------------


def _frame(table: str, records: list[dict[str, object]]) -> pd.DataFrame:
    """Build a DataFrame with exactly the architecture's columns, in order and typed.

    ``schema.py`` is the source of truth, and reindexing to it would silently turn a
    renamed column into an all-NULL one. Both directions of drift are therefore raised
    rather than reindexed away: a produced key the schema does not know, and a schema
    column nothing produced.
    """
    columns = schema.column_names(table)
    produced = {key for record in records for key in record}
    unknown = produced - set(columns)
    if unknown:
        raise ValueError(
            f"{table}: produced columns absent from schema.py: {sorted(unknown)}"
        )
    missing = set(columns) - produced
    if records and missing:
        raise ValueError(
            f"{table}: schema.py columns nothing produced: {sorted(missing)}"
        )
    frame = pd.DataFrame(records, columns=columns)
    for name, bigquery_type in schema.column_types(table).items():
        if bigquery_type == "INT64":
            frame[name] = pd.to_numeric(frame[name], errors="coerce").astype(
                "Int64"
            )
        elif bigquery_type == "FLOAT64":
            frame[name] = pd.to_numeric(frame[name], errors="coerce").astype(
                "Float64"
            )
        elif bigquery_type == "STRING":
            frame[name] = frame[name].astype("string")
    return frame


# --------------------------------------------------------------------------------------
# Orchestration
# --------------------------------------------------------------------------------------


def _resolve_cache_dir(cache_dir: str | Path) -> Path:
    """Accept either the input root or the website sub-directory."""
    path = Path(cache_dir)
    return path if path.name == "website" else path / "website"


def _log(message: str) -> None:
    print(message, flush=True)


def _collect_contests(fetcher: Fetcher) -> list[Contest]:
    """Every Legislative Assembly contest covered by this module, 2022 onwards."""
    contests: list[Contest] = []

    hub = fetcher.get_text(HUB_2022_URL, "hub2022.html")
    if hub is None:
        raise RuntimeError(
            f"could not fetch the 2022 results hub at {HUB_2022_URL}"
        )
    general = Event(
        election_id=ELECTION_2022_ID,
        election_name=ELECTION_2022_NAME,
        election_type="state_general",
        election_date=ELECTION_2022_DATE,
        source_url=HUB_2022_URL,
    )
    district_hrefs = _hrefs(hub, _DISTRICT_LINK_RE)
    _log(
        f"[hub] 2022 state election links {len(district_hrefs)} district pages"
    )
    for index, href in enumerate(sorted(district_hrefs), start=1):
        slug = href.rsplit("/", 1)[-1]
        document = fetcher.get_text(_absolute(href), f"d2022_{slug}.html")
        if document is None:
            continue
        contests.append(
            _parse_contest_page(document, general, _absolute(href))
        )
        if index % 20 == 0:
            _log(
                f"[hub]   parsed {index}/{len(district_hrefs)} district pages"
            )

    timeline = fetcher.get_text(BY_ELECTION_TIMELINE_URL, "byelections.html")
    if timeline is None:
        raise RuntimeError("could not fetch the by-election timeline")
    for event, href in _by_election_events(timeline):
        document = fetcher.get_text(
            _absolute(href), f"by_{event.election_id}.html"
        )
        if document is None:
            continue
        contests.append(_parse_contest_page(document, event, _absolute(href)))
        _log(
            f"[by-election] {event.election_id} — {event.election_name} "
            f"({event.election_date.isoformat()})"
        )
    return contests


def _indicative_records(
    fetcher: Fetcher, contests_by_key: dict[tuple[str, str], Contest]
) -> tuple[list[dict[str, object]], list[str]]:
    """Fetch and parse the 2023 statistical full-preference distributions."""
    page = fetcher.get_text(INDICATIVE_URL, "indicative.html")
    if page is None:
        return [], ["could not fetch the indicative distributions page"]

    records: list[dict[str, object]] = []
    problems: list[str] = []
    links = _MEDIA_LINK_RE.findall(_strip_chrome(page))
    _log(f"[indicative] {len(links)} workbook links")
    for href, anchor in links:
        label = _text_of(anchor)
        target = _indicative_target(label)
        if target is None:
            problems.append(f"unrecognised indicative label: {label!r}")
            continue
        kind, district = target
        if kind == "general":
            key = (ELECTION_2022_ID, district)
        elif kind == "supplementary":
            key = (
                ELECTION_ID_BY_PAGE[
                    "narracan-district-supplementary-election-results"
                ],
                district,
            )
        else:
            key = (f"{_slugify(district)}_by2023", district)
        contest = contests_by_key.get(key)
        if contest is None:
            problems.append(
                f"no contest for indicative workbook {label!r} (key={key})"
            )
            continue
        suffix = Path(unescape(href)).suffix.lower()
        path = fetcher.get_binary(
            _absolute(href), f"indicative_{contest.contest_id}{suffix}"
        )
        if path is None:
            continue
        rows = _workbook_rows(path)
        header_index = next(
            (
                index
                for index, row in enumerate(rows)
                if row
                and row[0].casefold().startswith(DOP_CANDIDATE_HEADER_PREFIX)
            ),
            None,
        )
        if header_index is None:
            problems.append(f"no candidate header row in {path.name}")
            continue
        records.extend(
            _distribution_records(
                contest,
                rows[header_index],
                tuple(rows[header_index + 1 :]),
            )
        )
    return records, problems


def parse_all(
    cache_dir: str, refresh: bool = False
) -> dict[str, pd.DataFrame]:
    """Scrape every Legislative Assembly count the VEC publishes only on its website.

    Returns one DataFrame per key: ``election``, ``result_district``,
    ``result_voting_centre``, ``distribution_of_preferences`` (the legal on-page
    distributions) and ``distribution_of_preferences_indicative`` (the 2023 statistical
    counts, kept separate so they are never published as legal results by accident).
    """
    fetcher = Fetcher(_resolve_cache_dir(cache_dir), refresh=refresh)

    contests = _collect_contests(fetcher)
    _log(f"[contests] {len(contests)} contests parsed")

    events: dict[str, Event] = {}
    result_district: list[dict[str, object]] = []
    voting_centre: list[dict[str, object]] = []
    distributions: list[dict[str, object]] = []
    with_distribution: list[str] = []
    without_distribution: list[str] = []
    with_two_party: list[str] = []
    formal_mismatch: list[str] = []
    centre_gap: list[str] = []
    unresolved_counts: list[str] = []
    dropped_buckets = 0

    for index, contest in enumerate(contests, start=1):
        events.setdefault(contest.event.election_id, contest.event)
        district_rows = _result_district_records(contest)
        result_district.extend(district_rows)
        if any(row["count_type"] == COUNT_TPP for row in district_rows):
            with_two_party.append(contest.district_name)

        # ``votes`` is written by ``_to_int``, so it is ``int | None`` and nothing
        # else; the record dicts are typed ``object``, so the narrowing is spelled
        # out rather than left to an ``int()`` call that would also have silently
        # accepted (and truncated) a float.
        tcp_votes: list[int] = [
            votes
            for row in district_rows
            if row["count_type"] == COUNT_TCP
            and isinstance(votes := row["votes"], int)
        ]
        if contest.formal_votes is not None and tcp_votes:
            total = sum(tcp_votes)
            if total != contest.formal_votes:
                formal_mismatch.append(
                    f"{contest.contest_id}: TCP sum {total} vs formal {contest.formal_votes} "
                    f"(delta {total - contest.formal_votes})"
                )

        if contest.voting_centre_href is not None:
            document = fetcher.get_text(
                _absolute(contest.voting_centre_href),
                f"vc_{contest.contest_id}.html",
            )
            if document is not None:
                rows, dropped = _voting_centre_records(contest, document)
                voting_centre.extend(rows)
                dropped_buckets += len(dropped)
                centre_total = sum(
                    votes
                    for row in rows
                    if isinstance(votes := row["votes"], int)
                )
                district_total = sum(tcp_votes)
                if centre_total and district_total != centre_total:
                    centre_gap.append(
                        f"{contest.contest_id}: 2CP by voting centre {centre_total} "
                        f"vs district TCP {district_total} "
                        f"(delta {centre_total - district_total})"
                    )

        if contest.distribution_href is None:
            without_distribution.append(contest.contest_id)
        else:
            document = fetcher.get_text(
                _absolute(contest.distribution_href),
                f"dop_{contest.contest_id}.html",
            )
            if document is None:
                without_distribution.append(contest.contest_id)
            else:
                contest_rows = _distribution_from_page(contest, document)
                distributions.extend(contest_rows)
                with_distribution.append(contest.contest_id)
                if contest_rows:
                    last = contest_rows[-1]["count_number"]
                    if all(
                        row["votes_progressive_total"] is None
                        for row in contest_rows
                        if row["count_number"] == last
                    ):
                        unresolved_counts.append(
                            f"{contest.contest_id}: count {last} has no progressive total"
                        )
        if index % 20 == 0:
            _log(f"[contests]   {index}/{len(contests)} contests fetched")

    contests_by_key = {
        (contest.event.election_id, contest.district_name): contest
        for contest in contests
    }
    indicative, indicative_problems = _indicative_records(
        fetcher, contests_by_key
    )

    election_records: list[dict[str, object]] = [
        {
            "year": event.year,
            "election_id": event.election_id,
            "election_name": event.election_name,
            "election_type": event.election_type,
            "government_level": GOVERNMENT_LEVEL,
            "election_date": event.election_date,
            "source_url": event.source_url,
        }
        for event in sorted(
            events.values(), key=lambda item: item.election_date
        )
    ]

    _log("")
    _log("=== parse_website summary ===")
    _log(
        f"pages attempted: {fetcher.attempted}, newly downloaded: {fetcher.downloaded}"
    )
    _log(f"failures: {len(fetcher.failures)}")
    for url, reason in fetcher.failures:
        _log(f"  FAIL {reason} {url}")
    _log(f"contests: {len(contests)} across {len(events)} electoral events")
    _log(
        f"legal distribution pages: {len(with_distribution)} present, "
        f"{len(without_distribution)} absent"
    )
    if without_distribution:
        _log(f"  absent: {', '.join(sorted(without_distribution))}")
    _log(f"separate two-party-preferred table: {len(with_two_party)} contests")
    if with_two_party:
        _log(f"  {', '.join(sorted(with_two_party))}")
    _log(f"TCP-vs-formal-votes mismatches: {len(formal_mismatch)}")
    for line in formal_mismatch:
        _log(f"  {line}")
    _log(
        "contests where the 2CP-by-voting-centre total differs from the district TCP: "
        f"{len(centre_gap)}"
    )
    for line in centre_gap[:5]:
        _log(f"  {line}")
    _log(
        f"distributions ending without a progressive total: {len(unresolved_counts)}"
    )
    for line in unresolved_counts:
        _log(f"  {line}")
    _log(
        f"dropped all-zero '{VOTING_CENTRE_EMPTY_BUCKET}' rows: {dropped_buckets}"
    )
    for problem in indicative_problems:
        _log(f"  INDICATIVE {problem}")

    frames = {
        TABLE_ELECTION: _frame(TABLE_ELECTION, election_records),
        TABLE_RESULT_DISTRICT: _frame(TABLE_RESULT_DISTRICT, result_district),
        TABLE_RESULT_VOTING_CENTRE: _frame(
            TABLE_RESULT_VOTING_CENTRE, voting_centre
        ),
        TABLE_DOP: _frame(TABLE_DOP, distributions),
        KEY_DOP_INDICATIVE: _frame(TABLE_DOP, indicative),
    }
    for name, frame in frames.items():
        _log(f"{name}: {len(frame):,} rows")
    return frames


if __name__ == "__main__":
    from pipelines.datasets.au_vic_vec_elections.constants import data_root

    parse_all(str(data_root() / "input"))
