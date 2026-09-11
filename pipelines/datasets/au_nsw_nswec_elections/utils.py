"""Cleaning transform for the NSW Electoral Commission state election results.

Pure functions only — no Prefect imports — so the one-shot onboarding bootstrap in
``models/au_nsw_nswec_elections/code/`` and any later recurring pipeline share one
implementation of the transform.

The NSWEC publishes each electoral event as its own website and the layout changed
twice across the four events covered here, so most readers below are per-era:

===== ===================================================================
Event  What the source publishes
===== ===================================================================
2023   Bulk XLSX per chamber, per-district HTML, ballot-level TSV, venue registry
2019   Bulk XLSX per chamber, per-district HTML, ballot-level TSV
2015   Bulk XLSX per chamber, per-district HTML, ballot-level TSV
2011   Per-district HTML only. No voting-centre results, no distribution of
       preferences, no ballot-level data, no enrolment. Candidates carry a
       surname and a party code but no given names.
===== ===================================================================

Every table is written all-STRING to Parquet: staging is all-STRING by house
convention and the dbt model ``safe_cast``s each column to its architecture type.
"""

from __future__ import annotations

import io
import pathlib
import re
import zipfile
from collections.abc import Iterator, Mapping

import pandas as pd

from pipelines.datasets.au_nsw_nswec_elections.schema import (
    column_names,
)

# --------------------------------------------------------------------------------------
# Event catalogue
# --------------------------------------------------------------------------------------

EVENTS: dict[int, dict[str, str]] = {
    2011: {
        "election_id": "SGE2011",
        "date": "2011-03-26",
        "url": "https://pastvtr.elections.nsw.gov.au/SGE2011",
    },
    2015: {
        "election_id": "SGE2015",
        "date": "2015-03-28",
        "url": "https://pastvtr.elections.nsw.gov.au/SGE2015",
    },
    2019: {
        "election_id": "SG1901",
        "date": "2019-03-23",
        "url": "https://pastvtr.elections.nsw.gov.au/SG1901",
    },
    2023: {
        "election_id": "SG2301",
        "date": "2023-03-25",
        "url": "https://pastvtr.elections.nsw.gov.au/SG2301",
    },
}

BULK_YEARS = (2015, 2019, 2023)
LA = "legislative_assembly"
LC = "legislative_council"

# The 2011 first-preference pages publish three separate stages of the count. Every
# later event publishes the final figures only.
COUNT_STAGES_2011 = {
    "Election Night": "election_night",
    "Check Count": "check_count",
    "Check Count & Dec": "check_count_and_declaration",
}


def slugify(name: str) -> str:
    """District display name to the slug the NSWEC uses in its own URLs."""
    # pyrefly: ignore [unnecessary-type-conversion]
    return re.sub(r"[^a-z0-9]+", "-", str(name).strip().lower()).strip("-")


def contest_id(chamber: str, district: str) -> str:
    return f"la-{slugify(district)}" if chamber == LA else "lc-state"


def strip_marker(value: str | None) -> str | None:
    """Drop the trailing ``+`` the 2011 pages append to sitting candidates and parties.

    The 2011 legend reads "+ Sitting Candidate or Party": the mark is presentation,
    not part of the name, and it is absent from every later event. Leaving it in place
    would make ``APLIN+`` and ``APLIN`` two different people across events.
    """
    if value is None:
        return None
    return value.rstrip("+").strip() or None


def _text(value: object) -> str | None:
    """Normalise a scraped cell to a clean string, or None when it carries no value."""
    if value is None:
        return None
    s = str(value).replace("\xa0", " ").strip()
    if s in ("", "nan", "None", "NaN", "-", "\u2013", "*"):
        return None
    return re.sub(r"\s+", " ", s)


def _number(value: object) -> str | None:
    """Strip thousands separators, percent signs and footnote marks from a figure."""
    s = _text(value)
    if s is None:
        return None
    s = s.replace(",", "").replace("%", "").replace("$", "").strip()
    # The distribution pages write "ELECTED 26,368" and "EXCLUDED" into the vote
    # cells. Strip the marker words, then insist on a real number: salvaging digits
    # out of anything else turned "ELECTED 26368" into "EEE26368".
    s = re.sub(r"(?i)\b(elected|excluded)\b", "", s).strip()
    try:
        float(s)
    except ValueError:
        return None
    return s


def _is_shouty(word: str) -> bool:
    """True for a word printed in surname case, false for an ordinary Titlecase name.

    ``McBRIDE``, ``O'BRIEN`` and ``BURNUM`` are surname case; ``Cheryl`` and
    ``Marelle`` are not. The test is "carries an upper-case letter after the first",
    which is what separates the NSWEC's upper-case surnames from given names without
    special-casing every Celtic and Dutch prefix by hand.
    """
    letters = [c for c in word if c.isalpha()]
    return len(letters) > 1 and any(c.isupper() for c in letters[1:])


def split_ballot_name(
    ballot_name: str | None,
) -> tuple[str | None, str | None]:
    """Split "SURNAME Given names" on the last surname-case word.

    The NSWEC prints the surname in upper case, which separates the two parts without
    ambiguity. Taking the *last* surname-case word as the boundary keeps multi-word
    surnames whole, including any lower-case particles inside them (``van der WATER
    Jan`` splits after ``WATER``, not before ``van``).
    """
    s = _text(ballot_name)
    if s is None:
        return None, None
    words = s.split(" ")
    cut = -1
    for i, word in enumerate(words):
        if _is_shouty(word):
            cut = i
    if cut < 0 or cut == len(words) - 1:
        return s, None
    return " ".join(words[: cut + 1]), " ".join(words[cut + 1 :])


# --------------------------------------------------------------------------------------
# State electoral division crosswalk
# --------------------------------------------------------------------------------------


def load_sed_crosswalk(path: pathlib.Path) -> dict[str, str]:
    """Read the ``name -> id_state_electoral_division`` map for the 2021 ASGS vintage.

    One vintage is pinned deliberately. The ABS re-uses division ids across vintages
    with different meanings — ``10012`` is Burrinjuck in 2011 and Cabramatta in 2016
    and 2021 — so a column that mixed vintages would silently merge unrelated
    divisions on a group-by. Pinning 2021 keeps a single id space at the cost of
    leaving districts with no 2021 namesake unlinked, which is the honest outcome.
    """
    frame = pd.read_csv(path, dtype=str)
    return dict(
        zip(frame["name"], frame["id_state_electoral_division"], strict=True)
    )


# --------------------------------------------------------------------------------------
# HTML helpers
# --------------------------------------------------------------------------------------


def read_tables(path: pathlib.Path) -> list[pd.DataFrame]:
    if not path.exists():
        return []
    try:
        return pd.read_html(path, flavor="lxml")
    except ValueError:
        return []


def flatten_columns(frame: pd.DataFrame) -> pd.DataFrame:
    frame = frame.copy()
    frame.columns = [
        " | ".join(str(x) for x in col if "Unnamed" not in str(x))
        if isinstance(col, tuple)
        # The stubs type a non-tuple label as ``str``; pandas allows any hashable, and
        # these workbooks do carry integer header cells. The cast is load-bearing.
        # pyrefly: ignore [unnecessary-type-conversion]
        else str(col)
        for col in frame.columns
    ]
    return frame


def district_slugs(root: pathlib.Path, year: int) -> list[str]:
    return [
        line
        for line in (root / str(year) / "districts.txt")
        .read_text(encoding="utf-8")
        .splitlines()
        if line.strip()
    ]


# --------------------------------------------------------------------------------------
# Source readers
# --------------------------------------------------------------------------------------


def read_bulk_la(root: pathlib.Path, year: int) -> pd.DataFrame:
    """Legislative Assembly first preferences by voting centre, from the bulk XLSX."""
    frame = pd.read_excel(
        root / str(year) / "la_xlsx.xlsx", sheet_name="Data", dtype=str
    )
    # pyrefly: ignore [unnecessary-type-conversion]
    frame.columns = [str(c).strip() for c in frame.columns]
    return frame


def bulk_la_formal(root: pathlib.Path, year: int) -> pd.DataFrame:
    """The bulk sheet with its informal rows dropped.

    Each venue carries one informal row, and the NSWEC gives it the placeholder
    candidate ``zz-Informal`` with the party ``Informal``. It is a venue total, not a
    candidature, so anything reading candidates must exclude it or it becomes a
    candidate in every district.
    """
    frame = read_bulk_la(root, year)
    return frame[frame["Formal/Informal"].str.strip().str.lower() == "formal"]


def read_bulk_lc(root: pathlib.Path, year: int) -> pd.DataFrame:
    """Legislative Council above-the-line group votes by voting centre, wide by group."""
    frame = pd.read_excel(
        root / str(year) / "lc_xlsx.xlsx", sheet_name="LC", dtype=str
    )
    # pyrefly: ignore [unnecessary-type-conversion]
    frame.columns = [str(c).strip() for c in frame.columns]
    return frame


def district_names(root: pathlib.Path, year: int) -> dict[str, str]:
    """``slug -> display name``, taken from whichever source names every district."""
    if year in BULK_YEARS:
        names = read_bulk_la(root, year)["District"].dropna().unique()
        return {slugify(n): n for n in sorted({str(n).strip() for n in names})}
    # 2011 names its per-district pages after the district itself, keeping the
    # capitalisation and substituting an underscore for each space, so the slug is
    # read back off the file names rather than derived from the display name.
    out = {}
    for path in sorted((root / str(year) / "fp_summary").glob("*.html")):
        out[path.stem] = path.stem.replace("_", " ")
    return out


# Rows on the Legislative Council first preference page that are summaries, not
# candidatures. "Group Total" is a group's above the line votes plus every below the
# line vote for its candidates, so keeping it would double count the rows above it.
LC_SUMMARY_LABELS = frozenset({"GROUP TOTAL", "TOTAL VOTES / BALLOT PAPERS"})


def lc_groups(
    root: pathlib.Path, year: int
) -> dict[str, tuple[str | None, list[str]]]:
    """``group letter -> (registered group name, candidate ballot names in order)``.

    2015 to 2023 publish one HTML table per group on the Legislative Council first
    preference page: the group's own row carries the letter and, where the group
    registered one, its printed name; the rows beneath it are its candidates in ballot
    order. The final table is the ungrouped column, which has no letter.
    """
    out: dict[str, tuple[str | None, list[str]]] = {}
    for table in read_tables(root / str(year) / "lc_fp_page.html"):
        if "Group" not in table.columns:
            continue
        letter = None
        group_name = None
        candidates: list[str] = []
        for _, row in table.iterrows():
            cell = _text(row.get("Group"))
            label = _text(row.get("Candidates in Ballot Order"))
            if cell is not None:
                letter, group_name = cell, label
            elif label is not None:
                if label.upper() == "UNGROUPED CANDIDATES":
                    letter, group_name = "UG", None
                elif label.upper() not in LC_SUMMARY_LABELS:
                    candidates.append(label)
        if letter is not None:
            out[letter] = (group_name, candidates)
    return out


def lc_group_votes(root: pathlib.Path, year: int) -> pd.DataFrame:
    """Group and candidate first preference totals for the Legislative Council."""
    rows: list[dict[str, object]] = []
    for table in read_tables(root / str(year) / "lc_fp_page.html"):
        if "Group" not in table.columns:
            continue
        letter = None
        for _, row in table.iterrows():
            cell = _text(row.get("Group"))
            label = _text(row.get("Candidates in Ballot Order"))
            votes = _number(row.get("Votes / Ballot Papers"))
            pct = _number(row.get("% of Votes / Ballot Papers"))
            quota = _number(row.get("Number of Quotas"))
            if cell is not None:
                letter = cell
                rows.append(
                    {
                        "group_code": letter,
                        "group_name": label,
                        "ballot_name": None,
                        "votes": votes,
                        "percentage": pct,
                        "quota_count": quota,
                        "count_type": "above_the_line",
                    }
                )
            elif label is not None and label.upper() == "UNGROUPED CANDIDATES":
                letter = "UG"
            elif label is not None and label.upper() not in LC_SUMMARY_LABELS:
                rows.append(
                    {
                        "group_code": letter,
                        "group_name": None,
                        "ballot_name": label,
                        "votes": votes,
                        "percentage": pct,
                        "quota_count": quota,
                        "count_type": "first_preference",
                    }
                )
    return pd.DataFrame(rows)


# --------------------------------------------------------------------------------------
# Contest block
# --------------------------------------------------------------------------------------


def contest_block(
    year: int, chamber: str, district: str | None, sed: dict[str, str]
) -> dict[str, str | None]:
    """The nine columns every results table carries, denormalised onto each row.

    Contest attributes are denormalised rather than normalised into a ``contest``
    table so that the shape matches ``au_qld_ecq_elections`` and a cross-jurisdiction
    union stays a dbt model.
    """
    name = "State" if chamber == LC else district
    return {
        "year": str(year),
        "election_id": EVENTS[year]["election_id"],
        "contest_id": contest_id(chamber, district or "state"),
        "state_electoral_division_id": sed.get(name)
        if chamber == LA and name is not None
        else None,
        "chamber": chamber,
        "government_level": "state",
        "contest_type": "state_district"
        if chamber == LA
        else "state_at_large",
        "voting_system": "optional_preferential"
        if chamber == LA
        else "single_transferable_vote",
        "district_name": name,
    }


# --------------------------------------------------------------------------------------
# election
# --------------------------------------------------------------------------------------


def build_election(root: pathlib.Path) -> pd.DataFrame:
    rows = []
    for year, event in EVENTS.items():
        rows.append(
            {
                "year": str(year),
                "election_id": event["election_id"],
                "election_name": f"New South Wales State General Election {year}",
                "election_type": "State General",
                "government_level": "state",
                "election_date": event["date"],
                "results_archive_url": event["url"],
                "assembly_districts": str(len(district_names(root, year))),
                # Half of the 42 Legislative Council seats is renewed at each general
                # election. The number has been 21 at every event covered here.
                "council_seats_contested": "21",
            }
        )
    return pd.DataFrame(rows)


# --------------------------------------------------------------------------------------
# candidate
# --------------------------------------------------------------------------------------


def build_candidate(root: pathlib.Path, sed: dict[str, str]) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for year in EVENTS:
        names = district_names(root, year)
        elected = _elected_la(root, year)
        if year in BULK_YEARS:
            bulk = bulk_la_formal(root, year)
            keys = [
                "District",
                "Candidate Ballot Name",
                "Party Acronym",
                "Party Name",
            ]
            for _, row in bulk[keys].drop_duplicates().iterrows():
                district = _text(row["District"])
                ballot = _text(row["Candidate Ballot Name"])
                if district is None or ballot is None:
                    continue
                rows.append(
                    {
                        **contest_block(year, LA, district, sed),
                        "ballot_order_number": None,
                        "ballot_name": ballot,
                        "party_code": _text(row["Party Acronym"]),
                        "party_name": _text(row["Party Name"]),
                        "group_code": None,
                        "group_name": None,
                        "is_declared_elected": "yes"
                        if (district, ballot, None) in elected
                        else "no",
                        "elected_at_count": None,
                    }
                )
        else:
            for slug, district in names.items():
                tables = read_tables(
                    root / str(year) / "fp_summary" / f"{slug}.html"
                )
                if len(tables) < 2:
                    continue
                frame = flatten_columns(tables[1])
                for _, row in frame.iterrows():
                    ballot = strip_marker(_text(row.iloc[0]))
                    party = strip_marker(_text(row.iloc[1]))
                    if ballot is None or ballot == party:
                        continue  # the trailing total rows repeat their own label
                    rows.append(
                        {
                            **contest_block(year, LA, district, sed),
                            "ballot_order_number": None,
                            "ballot_name": ballot,
                            "party_code": party,
                            "party_name": None,
                            "group_code": None,
                            "group_name": None,
                            "is_declared_elected": "yes"
                            if (district, ballot, party) in elected
                            else "no",
                            "elected_at_count": None,
                        }
                    )
        rows.extend(_lc_candidates(root, year, sed))

    frame = pd.DataFrame(rows)
    surnames = frame["ballot_name"].map(split_ballot_name)
    frame["candidate_surname"] = [s for s, _ in surnames]
    frame["candidate_given_names"] = [g for _, g in surnames]
    return frame[column_names("candidate")]


def _elected_la(
    root: pathlib.Path, year: int
) -> set[tuple[str, str, str | None]]:
    """``(district, ballot name, party)`` of the member returned in each district.

    The party is part of the key because 2011 identifies candidates by surname
    alone; it is None for the later events, whose index page carries no party.
    """
    if year == 2011:
        # The 2011 index reports the election-night standing, including seats still
        # undecided that night. The two candidate preferred table on each district
        # page is the settled result, so the winner is read from there instead.
        # 2011 identifies candidates by surname alone, and two districts ran two
        # candidates of the same surname (Epping: SMITH; Hawkesbury: WILLIAMS), so
        # the party has to be part of the match or both are marked elected.
        out = set()
        for slug, district in district_names(root, year).items():
            tables = read_tables(
                root / str(year) / "fp_summary" / f"{slug}.html"
            )
            if not tables:
                continue
            tcp = flatten_columns(tables[0])
            best, best_votes = None, -1.0
            for _, row in tcp.iterrows():
                name = strip_marker(_text(row.iloc[0]))
                party = strip_marker(_text(row.iloc[1]))
                votes = _number(row.iloc[2])
                if name and votes is not None and float(votes) > best_votes:
                    surname, _given = split_ballot_name(name)
                    best, best_votes = (surname or name, party), float(votes)
            if best:
                out.add((district, *best))
        return out
    tables = read_tables(root / str(year) / "elected_page.html")
    if not tables:
        return set()
    frame = tables[0]
    return {
        (d, c, None)
        for d, c in zip(
            [_text(x) for x in frame["District"]],
            [_text(x) for x in frame["Candidate"]],
            strict=True,
        )
        if d and c
    }


def _lc_candidates(
    root: pathlib.Path, year: int, sed: dict[str, str]
) -> list[dict[str, object]]:
    """Legislative Council candidatures, which are a single statewide contest.

    2011 publishes only the 21 members returned, so that event contributes the elected
    candidates and nothing else; the later events publish every candidature in ballot
    order alongside the group that nominated it.
    """
    block = contest_block(year, LC, None, sed)
    elected = _elected_lc(root, year)
    rows: list[dict[str, object]] = []
    if year == 2011:
        for ballot, (group_code, group_name, at_count) in elected.items():
            rows.append(
                {
                    **block,
                    "ballot_order_number": None,
                    "ballot_name": ballot,
                    "party_code": None,
                    "party_name": None,
                    "group_code": group_code,
                    "group_name": group_name,
                    "is_declared_elected": "yes",
                    "elected_at_count": at_count,
                }
            )
        return rows
    for letter, (group_name, candidates) in lc_groups(root, year).items():
        for order, ballot in enumerate(candidates, 1):
            hit = elected.get(ballot)
            rows.append(
                {
                    **block,
                    "ballot_order_number": str(order),
                    "ballot_name": ballot,
                    "party_code": None,
                    "party_name": None,
                    "group_code": letter,
                    "group_name": group_name,
                    "is_declared_elected": "yes" if hit else "no",
                    "elected_at_count": hit[2] if hit else None,
                }
            )
    return rows


def _elected_lc(
    root: pathlib.Path, year: int
) -> dict[str, tuple[str | None, str | None, str | None]]:
    page = "lc_finalcount.html" if year == 2011 else "lc_elected_page.html"
    tables = read_tables(root / str(year) / page)
    if not tables:
        return {}
    frame = flatten_columns(tables[0])
    out = {}
    for _, row in frame.iterrows():
        values = [_text(x) for x in row]
        # Both layouts read: sequence, candidate name, group, group name, count.
        _, ballot, group, group_name, at_count = (values + [None] * 5)[:5]
        if ballot:
            out[ballot] = (group, group_name, _number(at_count))
    return out


# --------------------------------------------------------------------------------------
# result_voting_centre
# --------------------------------------------------------------------------------------

# Columns of the Legislative Council bulk sheet that are venue totals rather than a
# group's votes. Everything else in the header is a group letter.
LC_TOTAL_COLUMNS = ("ATL", "BTL", "Formal", "Informal", "Total")
LC_FIXED_COLUMNS = (
    "District",
    "Vote Type",
    "Vote Sub Type",
    "Venue/Declaration Name",
)


def build_result_voting_centre(
    root: pathlib.Path, sed: dict[str, str]
) -> pd.DataFrame:
    """Votes at each voting centre and declaration vote type.

    2011 published no voting-centre breakdown at all, so this table starts at 2015.
    """
    rows: list[dict[str, object]] = []
    for year in BULK_YEARS:
        rows.extend(_rvc_assembly(root, year, sed))
        rows.extend(_rvc_council(root, year, sed))
    return pd.DataFrame(rows)[column_names("result_voting_centre")]


def _rvc_assembly(
    root: pathlib.Path, year: int, sed: dict[str, str]
) -> list[dict[str, object]]:
    bulk = read_bulk_la(root, year)
    # The sheet carries one row per candidate plus one Informal row per venue. The
    # venue totals are rebuilt from those rows and repeated on every candidate row,
    # which is the shape the Legislative Council sheet already publishes.
    bulk = bulk.assign(
        _votes=[float(_number(v) or 0) for v in bulk["Final FP Votes"]]
    )
    venue_keys = [
        "District",
        "Vote Type",
        "Vote Sub Type",
        "Venue/Declaration Name",
    ]
    formal = bulk["Formal/Informal"].str.strip().str.lower() == "formal"
    totals = (
        bulk.assign(
            _formal=bulk["_votes"].where(formal, 0.0),
            _informal=bulk["_votes"].where(~formal, 0.0),
        )
        .groupby(venue_keys, dropna=False)[["_formal", "_informal"]]
        .sum()
    )
    out: list[dict[str, object]] = []
    for _, row in bulk[formal].iterrows():
        district = _text(row["District"])
        venue = _text(row["Venue/Declaration Name"])
        if district is None or venue is None:
            continue
        key = tuple(row[k] for k in venue_keys)
        total_formal, total_informal = totals.loc[key]
        out.append(
            {
                **contest_block(year, LA, district, sed),
                "voting_centre_district_name": district,
                "voting_centre_name": venue,
                "vote_type_code": _text(row["Vote Type"]),
                "vote_sub_type": _text(row["Vote Sub Type"]),
                "count_type": "first_preference",
                "ballot_order_number": None,
                "ballot_name": _text(row["Candidate Ballot Name"]),
                "party_code": _text(row["Party Acronym"]),
                "party_name": _text(row["Party Name"]),
                "group_code": None,
                "group_name": None,
                "votes": _number(row["Final FP Votes"]),
                "votes_above_the_line": None,
                "votes_below_the_line": None,
                "votes_formal": str(int(total_formal)),
                "votes_informal": str(int(total_informal)),
                "votes_total": str(int(total_formal + total_informal)),
            }
        )
    return out


def _rvc_council(
    root: pathlib.Path, year: int, sed: dict[str, str]
) -> list[dict[str, object]]:
    bulk = read_bulk_lc(root, year)
    groups = lc_groups(root, year)
    letters = [
        c
        for c in bulk.columns
        if c not in LC_FIXED_COLUMNS and c not in LC_TOTAL_COLUMNS
    ]
    out: list[dict[str, object]] = []
    for _, row in bulk.iterrows():
        district = _text(row["District"])
        venue = _text(row["Venue/Declaration Name"])
        if district is None or venue is None:
            continue
        # The Council is a single statewide contest, but its ballot papers are still
        # taken and tallied district by district. The row therefore keeps the
        # statewide contest_id and names the district the voting centre serves — the
        # same meaning district_name carries in the voting_centre table. Without it
        # every "Postal" and "Absent" row would collide across 93 districts.
        block = contest_block(year, LC, None, sed)
        block["state_electoral_division_id"] = sed.get(district)
        shared = {
            "voting_centre_district_name": district,
            "voting_centre_name": venue,
            "vote_type_code": _text(row["Vote Type"]),
            "vote_sub_type": _text(row["Vote Sub Type"]),
            "count_type": "above_the_line",
            "ballot_order_number": None,
            "ballot_name": None,
            "party_code": None,
            "party_name": None,
            "votes_above_the_line": _number(row.get("ATL")),
            "votes_below_the_line": _number(row.get("BTL")),
            "votes_formal": _number(row.get("Formal")),
            "votes_informal": _number(row.get("Informal")),
            "votes_total": _number(row.get("Total")),
        }
        for letter in letters:
            # 2015 labels its ungrouped column X; every later event labels it UG on
            # the results pages. Both are normalised to UG.
            code = "UG" if letter in ("X", "UG") else letter
            out.append(
                {
                    **block,
                    **shared,
                    "group_code": code,
                    "group_name": (groups.get(code) or (None, []))[0],
                    "votes": _number(row[letter]),
                }
            )
    return out


# --------------------------------------------------------------------------------------
# result_district
# --------------------------------------------------------------------------------------


def build_result_district(
    root: pathlib.Path, sed: dict[str, str]
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for year in EVENTS:
        if year in BULK_YEARS:
            rows.extend(_rd_assembly_modern(root, year, sed))
        else:
            rows.extend(_rd_assembly_2011(root, year, sed))
        rows.extend(_rd_council(root, year, sed))
    return pd.DataFrame(rows)[column_names("result_district")]


def _rd_row(
    block: Mapping[str, object], **kwargs: object
) -> dict[str, object]:
    base: dict[str, object] = {
        **block,
        "count_status": "final",
        "count_type": "first_preference",
        "ballot_order_number": None,
        "ballot_name": None,
        "party_code": None,
        "party_name": None,
        "group_code": None,
        "group_name": None,
        "votes": None,
        "percentage": None,
        "quota_count": None,
    }
    base.update(kwargs)
    return base


# The district first preference table closes with a total row that the parser sees as
# another candidate. It is the only upper-case-only label on any of the 279 pages, and
# leaving it in doubles every district's first preference total.
FP_SUMMARY_TOTAL_LABEL = "TOTAL FORMAL VOTES"


def _rd_assembly_modern(
    root: pathlib.Path, year: int, sed: dict[str, str]
) -> list[dict[str, object]]:
    """District totals for 2015 onward: first preferences, then the final DoP count."""
    out = []
    for slug, district in district_names(root, year).items():
        block = contest_block(year, LA, district, sed)
        tables = read_tables(root / str(year) / "fp_summary" / f"{slug}.html")
        if tables:
            for _, row in tables[0].iterrows():
                name = _text(row.get("Candidate"))
                if name is None or name.upper() == FP_SUMMARY_TOTAL_LABEL:
                    continue
                out.append(
                    _rd_row(
                        block,
                        ballot_name=name,
                        party_name=_text(row.get("Representation")),
                        votes=_number(row.get("Formal Votes")),
                        percentage=_number(row.get("% Total Formal Votes")),
                    )
                )
        out.extend(_tcp_from_dop(root, year, slug, block))
    return out


def _rd_assembly_2011(
    root: pathlib.Path, year: int, sed: dict[str, str]
) -> list[dict[str, object]]:
    """2011 publishes three stages of the first preference count and its own TCP table.

    Every stage is kept rather than only the last: the NSWEC never republished a
    single final figure for 2011, and count_status is what tells the three apart.
    """
    out = []
    for slug, district in district_names(root, year).items():
        block = contest_block(year, LA, district, sed)
        tables = read_tables(root / str(year) / "fp_summary" / f"{slug}.html")
        if len(tables) < 2:
            continue
        tcp = flatten_columns(tables[0])
        for _, row in tcp.iterrows():
            name = strip_marker(_text(row.iloc[0]))
            if name is None:
                continue
            surname, _given = split_ballot_name(name)
            out.append(
                _rd_row(
                    block,
                    count_status="post_election_night",
                    count_type="two_candidate_preferred",
                    ballot_name=surname or name,
                    party_code=strip_marker(_text(row.iloc[1])),
                    votes=_number(row.iloc[2]),
                    percentage=_number(row.iloc[3]),
                )
            )
        fp = tables[1]
        # Column pairs run (%, votes) per stage, in the order of COUNT_STAGES_2011.
        for _, row in fp.iterrows():
            name = strip_marker(_text(row.iloc[0]))
            party = strip_marker(_text(row.iloc[1]))
            if name is None or name == party:
                continue
            for index, status in enumerate(COUNT_STAGES_2011.values()):
                out.append(
                    _rd_row(
                        block,
                        count_status=status,
                        ballot_name=name,
                        party_code=party,
                        percentage=_number(row.iloc[2 + index * 2]),
                        votes=_number(row.iloc[3 + index * 2]),
                    )
                )
    return out


def _tcp_from_dop(
    root: pathlib.Path, year: int, slug: str, block: Mapping[str, object]
) -> list[dict[str, object]]:
    """Derive the two candidate preferred result from the last distribution count.

    The NSWEC publishes no two *party* preferred count. The final count of the
    distribution of preferences opposes the last two candidates standing, which is
    exactly the two candidate preferred result, so it is read from there rather than
    from the separate TCP page, which is rendered client side and carries no table.
    """
    dop = _dop_frame(root, year, slug)
    if dop is None or dop.empty:
        return []
    last = dop[dop["distribution_number"] == dop["distribution_number"].max()]
    out = []
    for _, row in last.iterrows():
        if row["votes_progressive_total"] is None:
            continue
        out.append(
            _rd_row(
                block,
                count_type="two_candidate_preferred",
                ballot_name=row["ballot_name"],
                party_code=row["party_code"],
                votes=row["votes_progressive_total"],
            )
        )
    return out


def _rd_council(
    root: pathlib.Path, year: int, sed: dict[str, str]
) -> list[dict[str, object]]:
    block = contest_block(year, LC, None, sed)
    if year == 2011:
        # 2011 publishes group totals only, with no per-candidate breakdown.
        tables = read_tables(root / str(year) / "lc_summary.html")
        if not tables:
            return []
        frame = flatten_columns(tables[0])
        out = []
        for _, row in frame.iterrows():
            values = [_text(x) for x in row]
            code, name, _night, _dec, total, pct, quota = (
                values + [None] * 7
            )[:7]
            if code is None:
                continue
            out.append(
                _rd_row(
                    block,
                    count_type="above_the_line",
                    group_code="UG" if code.upper() in ("X", "UG") else code,
                    group_name=name,
                    votes=_number(total),
                    percentage=_number(pct),
                    quota_count=_number(quota),
                )
            )
        return out
    return [
        _rd_row(
            block,
            count_type=str(row["count_type"]),
            ballot_name=row["ballot_name"],
            group_code=row["group_code"],
            group_name=row["group_name"],
            votes=row["votes"],
            percentage=row["percentage"],
            quota_count=row["quota_count"],
        )
        for _, row in lc_group_votes(root, year).iterrows()
    ]


# --------------------------------------------------------------------------------------
# distribution_of_preferences
# --------------------------------------------------------------------------------------

# Row labels in the leftmost column of the distribution table that carry a count total
# rather than a candidate's position.
# Row labels in the leftmost column that carry a count total rather than a candidate's
# position, mapped to (output field, which column of the count pair to read).
# Each count after the first occupies two columns and the two carry different figures:
# "Total Votes in Count" is the number distributed under Votes Distributed and the
# number still live under Progressive Totals, while "Exhausted Votes" is the count's
# own figure and the running total respectively.
DOP_TOTAL_ROWS: dict[str, list[tuple[str, str]]] = {
    "Total Votes in Count": [("votes_in_count", "progressive")],
    "Exhausted Votes": [
        ("votes_exhausted", "distributed"),
        ("votes_exhausted_total", "progressive"),
    ],
    "Absolute Majority": [("absolute_majority", "progressive")],
    "Total Votes / Ballot Papers": [],
    "Informal": [],
}


def _dop_frame(
    root: pathlib.Path, year: int, slug: str
) -> pd.DataFrame | None:
    """Melt one district's distribution of preferences page into long form.

    The published table is wide: the first column names the candidates and the count
    totals, and every count occupies one column (count 1, the first preference count)
    or two (votes distributed, progressive totals). The header row above them names
    the candidate excluded at that count.
    """
    tables = read_tables(root / str(year) / "dop" / f"{slug}.html")
    if not tables:
        return None
    raw = tables[0]
    values = raw.to_numpy().tolist()
    if len(values) < 3:
        return None
    excluded_header, kind_header = values[0], values[1]
    body = values[2:]

    counts: dict[str, dict[str, object]] = {}
    for col in range(1, len(kind_header)):
        kind = _text(kind_header[col])
        label = _text(raw.columns[col])
        if kind is None or label is None or not label.startswith("Count"):
            continue
        number = label.split()[1].split(".")[0]
        entry = counts.setdefault(
            number,
            {"excluded": None, "distributed": None, "progressive": None},
        )
        excluded = _text(excluded_header[col])
        if excluded and "Excluded Candidate" in excluded:
            entry["excluded"] = excluded.replace(
                "Excluded Candidate", ""
            ).strip()
        if kind in ("Votes Distributed", "First Preference Votes"):
            entry["distributed"] = col
        elif kind == "Progressive Totals":
            entry["progressive"] = col

    totals: dict[str, dict[str, str | None]] = {n: {} for n in counts}
    candidates: list[tuple[str | None, str | None, list[object]]] = []
    for row in body:
        label = _text(row[0])
        if label is None:
            continue
        if label in DOP_TOTAL_ROWS:
            for field, which in DOP_TOTAL_ROWS[label]:
                for number, entry in counts.items():
                    # Count 1 has a single column, so a request for the progressive
                    # figure falls back to it.
                    col = entry[which] or entry["distributed"]
                    totals[number][field] = (
                        _number(row[col]) if isinstance(col, int) else None
                    )
            continue
        name, party = _split_dop_label(label)
        candidates.append((name, party, row))

    rows = []
    for number in sorted(counts, key=int):
        entry = counts[number]
        # `counts` is heterogeneous by design: "excluded" holds a label, the other
        # two hold column indices. Narrow on read rather than splitting the dict.
        # The values are only ever None, a str label, or an int column index, so
        # these isinstance tests select exactly what the bare truthiness tests they
        # replace did: columns are numbered from 1, so no index is ever falsy.
        excluded_label = entry["excluded"]
        excluded_name, excluded_party = _split_dop_label(
            excluded_label if isinstance(excluded_label, str) else None
        )
        for name, party, row in candidates:
            distributed_col, progressive_col = (
                entry["distributed"],
                entry["progressive"],
            )
            distributed = (
                _text(row[distributed_col])
                if isinstance(distributed_col, int)
                else None
            )
            progressive = (
                _text(row[progressive_col])
                if isinstance(progressive_col, int)
                else None
            )
            if number == "1":
                # Count 1 holds the first preference votes in a single column, and
                # marks the eventual winner inline as "ELECTED 26,368".
                progressive, distributed = distributed, distributed
            is_excluded = (
                distributed is not None and "EXCLUDED" in distributed.upper()
            )
            is_elected = (
                progressive is not None and "ELECTED" in progressive.upper()
            )
            if distributed is None and progressive is None:
                continue
            rows.append(
                {
                    "distribution_number": number,
                    "excluded_ballot_name": excluded_name,
                    "excluded_party_code": excluded_party,
                    "ballot_name": name,
                    "party_code": party,
                    "votes_transferred": _number(distributed),
                    "votes_progressive_total": _number(progressive),
                    "is_excluded": "yes" if is_excluded else "no",
                    "is_elected": "yes" if is_elected else "no",
                    "votes_in_count": totals[number].get("votes_in_count"),
                    "votes_exhausted": totals[number].get("votes_exhausted"),
                    "votes_exhausted_total": totals[number].get(
                        "votes_exhausted_total"
                    ),
                    "absolute_majority": totals[number].get(
                        "absolute_majority"
                    ),
                }
            )
    return pd.DataFrame(rows)


def _split_dop_label(label: str | None) -> tuple[str | None, str | None]:
    """Split "SURNAME Given (PTY)" or "SURNAME Given PTY" into name and party code.

    The excluded-candidate header parenthesises the party; the candidate rows do not,
    and 2015 runs the two together as ``MARRA John(NLT)``.
    """
    s = _text(label)
    if s is None:
        return None, None
    match = re.search(r"\(([^)]+)\)\s*$", s)
    if match:
        return _text(s[: match.start()]), match.group(1).strip()
    words = s.split(" ")
    if len(words) > 1 and words[-1].isupper() and len(words[-1]) <= 5:
        return " ".join(words[:-1]), words[-1]
    return s, None


def build_distribution_of_preferences(
    root: pathlib.Path, sed: dict[str, str]
) -> pd.DataFrame:
    """Preference transfers in the Assembly. 2011 published no distribution at all."""
    frames = []
    for year in BULK_YEARS:
        for slug, district in district_names(root, year).items():
            frame = _dop_frame(root, year, slug)
            if frame is None or frame.empty:
                continue
            for key, value in contest_block(year, LA, district, sed).items():
                frame[key] = value
            frames.append(frame)
    return pd.concat(frames, ignore_index=True)[
        column_names("distribution_of_preferences")
    ]


# --------------------------------------------------------------------------------------
# enrolment_turnout
# --------------------------------------------------------------------------------------


def build_enrolment_turnout(
    root: pathlib.Path, sed: dict[str, str]
) -> pd.DataFrame:
    """Enrolment and turnout per contest.

    2011 is absent: the NSWEC published no enrolment figure for that event, and a
    turnout row without a denominator would be a row of nulls.
    """
    # `_turnout_council` returns None when an event published no Council votes; the
    # comprehension at the end of the function drops those before the DataFrame.
    rows: list[dict[str, object] | None] = []
    for year in BULK_YEARS:
        candidates = _candidate_counts(root, year)
        tables = read_tables(root / str(year) / "turnout_page.html")
        if not tables:
            continue
        frame = tables[0]
        # pyrefly: ignore [unnecessary-type-conversion]
        columns = {str(c): c for c in frame.columns}
        enrolment_col = next(
            (c for c in columns if c.startswith("District Enrolment")), None
        )
        share_col = next(
            (
                c
                for c in columns
                if c.startswith("% Total Votes") or "Participation" in c
            ),
            None,
        )
        for _, row in frame.iterrows():
            district = _text(row["District"])
            if district is None or district.lower() in (
                "total",
                "state total",
            ):
                continue
            formal = _number(row["Formal Votes"])
            informal = _number(row["Informal Ballot Papers"])
            total = _number(row["Total Votes / Ballot Papers Counted"])
            rows.append(
                {
                    **contest_block(year, LA, district, sed),
                    "count_status": "final",
                    "number_to_elect": "1",
                    "candidates_count": candidates.get(district),
                    "enrolment": _number(row[enrolment_col])
                    if enrolment_col
                    else None,
                    "votes_total": total,
                    "votes_formal": formal,
                    "votes_informal": informal,
                    "percentage_informal": _number(row["% Informality"]),
                    "percentage_roll_counted": _number(row[share_col])
                    if share_col
                    else None,
                }
            )
        rows.append(_turnout_council(root, year, sed))
    return pd.DataFrame([r for r in rows if r])[
        column_names("enrolment_turnout")
    ]


def _candidate_counts(root: pathlib.Path, year: int) -> dict[str, str]:
    bulk = bulk_la_formal(root, year)
    counts = (
        bulk[["District", "Candidate Ballot Name"]]
        .dropna()
        .drop_duplicates()
        .groupby("District")
        .size()
    )
    return {str(k): str(v) for k, v in counts.items()}


def _turnout_council(
    root: pathlib.Path, year: int, sed: dict[str, str]
) -> dict[str, object] | None:
    """Statewide Council totals, summed from the bulk sheet's per-venue totals."""
    bulk = read_bulk_lc(root, year)
    formal = sum(float(_number(v) or 0) for v in bulk["Formal"])
    informal = sum(float(_number(v) or 0) for v in bulk["Informal"])
    total = formal + informal
    if total == 0:
        return None
    candidates = sum(len(c) for _, c in lc_groups(root, year).values())
    enrolment = sum(
        float(r["enrolment"] or 0)
        for r in _assembly_enrolment(root, year)
        if r["enrolment"]
    )
    return {
        **contest_block(year, LC, None, sed),
        "count_status": "final",
        "number_to_elect": "21",
        "candidates_count": str(candidates),
        "enrolment": str(int(enrolment)) or None,
        "votes_total": str(int(total)),
        "votes_formal": str(int(formal)),
        "votes_informal": str(int(informal)),
        "percentage_informal": f"{informal / total * 100:.2f}",
        "percentage_roll_counted": f"{total / enrolment * 100:.2f}"
        if enrolment
        else None,
    }


def _assembly_enrolment(
    root: pathlib.Path, year: int
) -> list[dict[str, str | None]]:
    tables = read_tables(root / str(year) / "turnout_page.html")
    if not tables:
        return []
    frame = tables[0]
    column = next(
        (
            # pyrefly: ignore [unnecessary-type-conversion]
            str(c)
            for c in frame.columns
            # pyrefly: ignore [unnecessary-type-conversion]
            if str(c).startswith("District Enrolment")
        ),
        None,
    )
    if column is None:
        return []
    return [
        {"enrolment": _number(row[column])}
        for _, row in frame.iterrows()
        if _text(row["District"])
        and str(_text(row["District"])).lower() not in ("total", "state total")
    ]


# --------------------------------------------------------------------------------------
# voting_centre
# --------------------------------------------------------------------------------------


def build_voting_centre(
    root: pathlib.Path, sed: dict[str, str]
) -> pd.DataFrame:
    """Every voting centre and declaration vote type each event actually used.

    The venue registry — address, accessibility, coordinates — is published for 2023
    only, so those columns are null for the earlier events. The registry is joined on
    the short venue name the results use, and centres that appear in the registry but
    never in the results are kept with ``is_in_results`` set to no.
    """
    rows: list[dict[str, object]] = []
    for year in BULK_YEARS:
        registry = _venue_registry(root) if year == 2023 else {}
        seen: set[tuple[str, str, str | None]] = set()
        for chamber_frame in (
            read_bulk_la(root, year),
            read_bulk_lc(root, year),
        ):
            for _, row in chamber_frame.iterrows():
                district = _text(row["District"])
                venue = _text(row["Venue/Declaration Name"])
                sub_type = _text(row["Vote Sub Type"])
                if district is None or venue is None:
                    continue
                key = (district, venue, sub_type)
                if key in seen:
                    continue
                seen.add(key)
                detail = registry.get((district, venue), {})
                rows.append(
                    {
                        "year": str(year),
                        "election_id": EVENTS[year]["election_id"],
                        "state_electoral_division_id": sed.get(district),
                        "district_name": district,
                        "voting_centre_name": venue,
                        "vote_type_code": _text(row["Vote Type"]),
                        "vote_sub_type": sub_type,
                        "is_in_results": "yes",
                        **_registry_fields(detail),
                    }
                )
        for (district, venue), detail in registry.items():
            if any(k[0] == district and k[1] == venue for k in seen):
                continue
            rows.append(
                {
                    "year": str(year),
                    "election_id": EVENTS[year]["election_id"],
                    "state_electoral_division_id": sed.get(district),
                    "district_name": district,
                    "voting_centre_name": venue,
                    "vote_type_code": detail.get("vote_type_code"),
                    "vote_sub_type": detail.get("vote_sub_type"),
                    "is_in_results": "no",
                    **_registry_fields(detail),
                }
            )
    return pd.DataFrame(rows)[column_names("voting_centre")]


def _registry_fields(detail: dict[str, object]) -> dict[str, object]:
    return {
        "building_name": detail.get("building_name"),
        "address": detail.get("address"),
        "locality": detail.get("locality"),
        "postcode": detail.get("postcode"),
        "is_wheelchair_accessible": detail.get("is_wheelchair_accessible"),
        "latitude": detail.get("latitude"),
        "longitude": detail.get("longitude"),
    }


REGISTRY_FILES = (
    ("votingcentres-sge2023.xlsx", "PP", "Voting Centre"),
    ("earlyvotingcentres-sge2023.xlsx", "PR", "Early Voting Centre"),
    ("declaredfacilties-sge2023.xlsx", "DI", "Declared Facility"),
)


def _venue_registry(
    root: pathlib.Path,
) -> dict[tuple[str, str], dict[str, object]]:
    out: dict[tuple[str, str], dict[str, object]] = {}
    for name, type_code, sub_type in REGISTRY_FILES:
        path = root / "aux" / name
        if not path.exists():
            continue
        frame = pd.read_excel(path, sheet_name=1, dtype=str)
        # pyrefly: ignore [unnecessary-type-conversion]
        frame.columns = [str(c).strip() for c in frame.columns]
        short = next(
            (
                c
                for c in frame.columns
                if c.startswith("Unique Short Venue Name")
            ),
            "Venue Name",
        )
        long = next(
            (
                c
                for c in frame.columns
                if c in ("Full Venue Name", "Long Venue Name")
            ),
            None,
        )
        address = next(
            (c for c in frame.columns if c in ("Address Line 1", "Address")),
            None,
        )
        for _, row in frame.iterrows():
            district = _text(row.get("AreaCode"))
            venue = _text(row.get(short))
            if district is None or venue is None:
                continue
            out[(district, venue)] = {
                "building_name": _text(row.get(long)) if long else None,
                "address": _text(row.get(address)) if address else None,
                "locality": _text(row.get("Locality")),
                "postcode": _text(row.get("Postcode")),
                # The 2023 registry carries no accessibility rating; the column is
                # kept for the events that may publish one later.
                "is_wheelchair_accessible": None,
                "latitude": _number(row.get("Latitude")),
                "longitude": _number(row.get("Longitude")),
                "vote_type_code": type_code,
                "vote_sub_type": sub_type,
            }
    return out


# --------------------------------------------------------------------------------------
# ballot_preference
# --------------------------------------------------------------------------------------


def iter_ballot_preference(
    root: pathlib.Path, year: int, sed: dict[str, str]
) -> Iterator[tuple[str, pd.DataFrame]]:
    """Yield ``(district slug, frame)`` for each Assembly district's ballot papers.

    One row per ballot paper per marked preference — 33.7 million rows across the
    three events that publish it, so the district is the unit of work and nothing
    larger is ever held in memory at once. 2011 published no ballot-level data.

    The published file carries no party, so it is joined on from the bulk sheet: the
    candidate ballot name is unique within a district.
    """
    parties = _party_by_district(root, year)
    block_by_district = {}
    for slug, district in district_names(root, year).items():
        block_by_district[slug] = contest_block(year, LA, district, sed)

    for path in sorted((root / str(year) / "pref").glob("*.zip")):
        slug = path.stem
        block = block_by_district.get(slug)
        if block is None:
            continue
        with zipfile.ZipFile(path) as archive:
            name = archive.namelist()[0]
            with archive.open(name) as handle:
                frame = pd.read_csv(
                    io.TextIOWrapper(
                        handle, encoding="utf-8", errors="replace"
                    ),
                    sep="\t",
                    dtype=str,
                    keep_default_na=False,
                    na_values=[""],
                )
        # pyrefly: ignore [unnecessary-type-conversion]
        frame.columns = [str(c).strip() for c in frame.columns]
        district = str(block["district_name"])
        lookup = parties.get(district, {})
        out = pd.DataFrame(
            {
                **{k: v for k, v in block.items()},
                "voting_centre_name": frame["PollingPlaceName"],
                "ballot_paper_id": frame["BPNumber"],
                "formality": frame["Formality"],
                "ballot_name": frame["CandidateName"],
                "party_code": frame["CandidateName"].map(lookup),
                "preference_number": frame["PrefMarking"],
                "preference_counted_number": frame["PrefCounted"],
            }
        )
        yield slug, out[column_names("ballot_preference")]


def _party_by_district(
    root: pathlib.Path, year: int
) -> dict[str, dict[str, str | None]]:
    bulk = bulk_la_formal(root, year)
    # Independents carry no party acronym, so the value is legitimately None. It is
    # kept rather than dropped: `frame["CandidateName"].map(lookup)` must still
    # resolve the candidate and yield a null party_code.
    out: dict[str, dict[str, str | None]] = {}
    for _, row in (
        bulk[["District", "Candidate Ballot Name", "Party Acronym"]]
        .drop_duplicates()
        .iterrows()
    ):
        district = _text(row["District"])
        ballot = _text(row["Candidate Ballot Name"])
        if district is None or ballot is None:
            continue
        out.setdefault(district, {})[ballot] = _text(row["Party Acronym"])
    return out


# --------------------------------------------------------------------------------------
# dicionario
# --------------------------------------------------------------------------------------

# Every coded column in the dataset, with the meaning of each stored key. Codes the
# NSWEC changed between events carry the coverage of the events that used them.
DICTIONARY: list[tuple[str, str, str, str, str]] = [
    (
        "todas",
        "chamber",
        "legislative_assembly",
        "2011(4)2023",
        "Legislative Assembly",
    ),
    (
        "todas",
        "chamber",
        "legislative_council",
        "2011(4)2023",
        "Legislative Council",
    ),
    ("todas", "government_level", "state", "2011(4)2023", "State government"),
    (
        "todas",
        "contest_type",
        "state_district",
        "2011(4)2023",
        "One seat per state electoral district",
    ),
    (
        "todas",
        "contest_type",
        "state_at_large",
        "2011(4)2023",
        "Single statewide contest",
    ),
    (
        "todas",
        "voting_system",
        "optional_preferential",
        "2011(4)2023",
        "Optional preferential voting",
    ),
    (
        "todas",
        "voting_system",
        "single_transferable_vote",
        "2011(4)2023",
        "Single transferable vote, above and below the line",
    ),
    (
        "election",
        "election_type",
        "State General",
        "2011(4)2023",
        "State general election",
    ),
    (
        "todas",
        "count_status",
        "final",
        "2015(4)2023",
        "Final count published by the NSWEC",
    ),
    (
        "todas",
        "count_status",
        "election_night",
        "2011(1)2011",
        "Count as at election night",
    ),
    (
        "todas",
        "count_status",
        "check_count",
        "2011(1)2011",
        "Count after the check count",
    ),
    (
        "todas",
        "count_status",
        "check_count_and_declaration",
        "2011(1)2011",
        "Count after the check count and declaration votes",
    ),
    (
        "result_district",
        "count_status",
        "post_election_night",
        "2011(1)2011",
        "Two candidate preferred count published after election night",
    ),
    (
        "todas",
        "count_type",
        "first_preference",
        "2011(4)2023",
        "First preference votes for a candidate",
    ),
    (
        "todas",
        "count_type",
        "above_the_line",
        "2011(4)2023",
        "Above the line group votes in the Legislative Council",
    ),
    (
        "result_district",
        "count_type",
        "two_candidate_preferred",
        "2011(4)2023",
        "Two candidate preferred count between the last two candidates standing",
    ),
    (
        "todas",
        "vote_type_code",
        "PP",
        "2011(4)2023",
        "Election day voting centre",
    ),
    ("todas", "vote_type_code", "PR", "2011(4)2023", "Early voting centre"),
    ("todas", "vote_type_code", "DI", "2011(4)2023", "Declared facility"),
    ("todas", "vote_type_code", "DV", "2011(4)2023", "Declaration vote"),
    (
        "todas",
        "vote_sub_type",
        "Voting Centre",
        "2019(2)2023",
        "Election day voting centre",
    ),
    (
        "todas",
        "vote_sub_type",
        "Polling Place",
        "2015(1)2015",
        "Election day voting centre, as the NSWEC labelled it in 2015",
    ),
    (
        "todas",
        "vote_sub_type",
        "Early Voting Centre",
        "2019(2)2023",
        "Early voting centre",
    ),
    (
        "todas",
        "vote_sub_type",
        "Pre-poll",
        "2015(1)2015",
        "Early voting centre, as the NSWEC labelled it in 2015",
    ),
    (
        "todas",
        "vote_sub_type",
        "Declared Facility",
        "2019(2)2023",
        "Hospital, nursing home or other declared facility",
    ),
    (
        "todas",
        "vote_sub_type",
        "Declared Institution",
        "2015(1)2015",
        "Declared facility, as the NSWEC labelled it in 2015",
    ),
    (
        "todas",
        "vote_sub_type",
        "Absent",
        "2015(3)2023",
        "Vote cast outside the elector's own district",
    ),
    ("todas", "vote_sub_type", "Postal", "2015(3)2023", "Postal vote"),
    (
        "todas",
        "vote_sub_type",
        "Enrolment / Provisional",
        "2019(2)2023",
        "Provisional vote, or a vote cast while enrolling",
    ),
    (
        "todas",
        "vote_sub_type",
        "Enrolment",
        "2015(1)2015",
        "Vote cast while enrolling",
    ),
    (
        "todas",
        "vote_sub_type",
        "Provisional/Silent",
        "2015(1)2015",
        "Provisional vote, or a vote by a silent elector",
    ),
    (
        "todas",
        "vote_sub_type",
        "iVote",
        "2015(2)2019",
        "Vote cast through the iVote internet and telephone channel, withdrawn after 2019",
    ),
    (
        "todas",
        "group_code",
        "UG",
        "2011(4)2023",
        "Ungrouped candidates on the Legislative Council ballot paper",
    ),
    ("todas", "is_declared_elected", "yes", "2011(4)2023", "Declared elected"),
    (
        "todas",
        "is_declared_elected",
        "no",
        "2011(4)2023",
        "Not declared elected",
    ),
    (
        "distribution_of_preferences",
        "is_excluded",
        "yes",
        "2015(3)2023",
        "Excluded at this count",
    ),
    (
        "distribution_of_preferences",
        "is_excluded",
        "no",
        "2015(3)2023",
        "Not excluded at this count",
    ),
    (
        "distribution_of_preferences",
        "is_elected",
        "yes",
        "2015(3)2023",
        "Standing elected after this count",
    ),
    (
        "distribution_of_preferences",
        "is_elected",
        "no",
        "2015(3)2023",
        "Not standing elected after this count",
    ),
    (
        "voting_centre",
        "is_in_results",
        "yes",
        "2015(3)2023",
        "Appears in the counted results of the event",
    ),
    (
        "voting_centre",
        "is_in_results",
        "no",
        "2015(3)2023",
        "In the venue registry but absent from the counted results",
    ),
    (
        "ballot_preference",
        "formality",
        "Formal",
        "2015(3)2023",
        "Formal ballot paper",
    ),
    (
        "ballot_preference",
        "formality",
        "Informal",
        "2015(3)2023",
        "Informal ballot paper, occupying a single row with no preferences",
    ),
]


def build_dicionario() -> pd.DataFrame:
    return pd.DataFrame(
        DICTIONARY,
        columns=[
            "id_tabela",
            "nome_coluna",
            "chave",
            "cobertura_temporal",
            "valor",
        ],
    )[column_names("dicionario")]
