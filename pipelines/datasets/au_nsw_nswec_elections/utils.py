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

import pathlib
import re

import pandas as pd

from pipelines.datasets.au_nsw_nswec_elections.schema import (
    column_names,
)

# --------------------------------------------------------------------------------------
# Event catalogue
# --------------------------------------------------------------------------------------

EVENTS: dict[int, dict[str, object]] = {
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
    s = re.sub(r"[^\d.\-+eE]", "", s)
    if s in ("", "-", "+", "."):
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
    frame.columns = [str(c).strip() for c in frame.columns]
    return frame


def read_bulk_lc(root: pathlib.Path, year: int) -> pd.DataFrame:
    """Legislative Council above-the-line group votes by voting centre, wide by group."""
    frame = pd.read_excel(
        root / str(year) / "lc_xlsx.xlsx", sheet_name="LC", dtype=str
    )
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
) -> dict[str, object]:
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
        if chamber == LA
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
            bulk = read_bulk_la(root, year)
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


def _elected_la(root: pathlib.Path, year: int) -> set[tuple[str, str]]:
    """``(district, ballot name)`` of the member returned in each Assembly district."""
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
