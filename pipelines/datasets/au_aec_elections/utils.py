"""Pure download and cleaning functions for au_aec_elections.

No Prefect imports live here: the one-shot onboarding bootstrap under
``models/au_aec_elections/code/`` and the recurring pipeline in ``flows.py`` both
import this module, so the transform exists in exactly one place.
"""

from __future__ import annotations

import csv
import re
import shutil
import time
import urllib.error
import urllib.request
import zipfile
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from pipelines.datasets.au_aec_elections import schema
from pipelines.datasets.au_aec_elections.constants import constants, data_root

UA = {"User-Agent": constants.USER_AGENT.value}

# The Transparency Register labels by-elections without a year ("Mayo by-election"),
# while the results archive labels them with one. Divisions that held only a single
# by-election resolve unambiguously from the event catalogue; the one division that
# held two needs an explicit answer.
#
# Mayo went to a by-election in both 2008 and 2018. The Transparency Register's single
# "Mayo by-election" block is the 2018 one: its candidates are Georgina Downer and
# Rebekha Sharkie for Centre Alliance, a party that did not exist in 2008.
_AMBIGUOUS_TRANSPARENCY_EVENTS = {"mayo by-election": 2018}


def _transparency_event_years() -> dict[str, int]:
    by_division: dict[str, list[int]] = {}
    for (
        year,
        _name,
        etype,
        division,
        _state,
    ) in constants.EXTRA_EVENTS.value.values():
        if etype != "by_election" or not division:
            continue
        by_division.setdefault(f"{division.lower()} by-election", []).append(
            year
        )
    resolved: dict[str, int] = {}
    for label, years in by_division.items():
        if label in _AMBIGUOUS_TRANSPARENCY_EVENTS:
            resolved[label] = _AMBIGUOUS_TRANSPARENCY_EVENTS[label]
        elif len(years) == 1:
            resolved[label] = years[0]
        # Anything left ambiguous stays unmapped, so it surfaces as a dropped-row
        # warning rather than being silently guessed.
    return resolved


TRANSPARENCY_EVENT_YEARS = _transparency_event_years()


# ======================================================================================
# Download
# ======================================================================================


def _get(url: str, tries: int = 3) -> bytes | None:
    for attempt in range(tries):
        try:
            req = urllib.request.Request(url, headers=UA)
            with urllib.request.urlopen(req, timeout=180) as r:
                return r.read()
        except urllib.error.HTTPError as e:
            if e.code == 404 or attempt == tries - 1:
                return None
        except Exception:
            if attempt == tries - 1:
                return None
        time.sleep(2 * (attempt + 1))
    return None


def download_file(url: str, dest: Path) -> bool:
    if dest.exists() and dest.stat().st_size > 0:
        return True
    data = _get(url)
    if data is None:
        return False
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(data)
    return True


def download_federal_election(event_id: int, root: Path | None = None) -> Path:
    """Download the full CSV set for one general election."""
    root = root or data_root()
    year, seg = constants.FEDERAL_ELECTIONS.value[event_id]
    base = f"{constants.RESULTS_BASE_URL.value}/{event_id}/{seg}/Downloads"
    out = root / "input" / "results" / str(year)
    for name in constants.NATIONAL_FILES.value:
        download_file(
            f"{base}/{name}-{event_id}.csv", out / f"{name}-{event_id}.csv"
        )
    for name in constants.BY_STATE_FILES.value:
        for st in constants.STATES.value:
            fn = f"{name}-{event_id}-{st}.csv"
            download_file(f"{base}/{fn}", out / fn)
    return out


def download_extra_event(event_id: int, root: Path | None = None) -> Path:
    """Download whatever a by-election, referendum or Senate-only event publishes.

    These events publish a smaller and event-specific file set, so the download menus
    are scraped rather than filenames guessed.
    """
    root = root or data_root()
    out = root / "input" / "results_extra" / str(event_id)
    out.mkdir(parents=True, exist_ok=True)
    prefix = f"{constants.RESULTS_BASE_URL.value}/{event_id}/Website"
    hrefs: set[str] = set()
    for menu in ("House", "Senate", "General", "Referendum"):
        body = _get(f"{prefix}/{menu}DownloadsMenu-{event_id}-Csv.htm")
        if body is None:
            continue
        html = body.decode("utf-8", errors="replace")
        hrefs.update(re.findall(r'href="([^"]+\.csv)"', html, flags=re.I))
    for href in sorted(hrefs):
        download_file(
            f"{prefix}/{href.lstrip('/')}", out / href.rsplit("/", 1)[-1]
        )
    return out


def download_transparency(root: Path | None = None) -> Path:
    """Download and unzip the three Transparency Register bulk bundles."""
    root = root or data_root()
    out = root / "input" / "transparency"
    out.mkdir(parents=True, exist_ok=True)
    for bundle in constants.TRANSPARENCY_BUNDLES.value:
        zp = out / f"{bundle}.zip"
        download_file(
            f"{constants.TRANSPARENCY_BASE_URL.value}/Download/{bundle}", zp
        )
        if zp.exists():
            target = out / bundle
            target.mkdir(parents=True, exist_ok=True)
            with zipfile.ZipFile(zp) as z:
                z.extractall(target)
    return out


def download_all(root: Path | None = None) -> None:
    root = root or data_root()
    for event_id in constants.FEDERAL_ELECTIONS.value:
        download_federal_election(event_id, root)
    for event_id in constants.EXTRA_EVENTS.value:
        download_extra_event(event_id, root)
    download_transparency(root)


# ======================================================================================
# Reading helpers
# ======================================================================================


def read_aec_csv(path: Path) -> pd.DataFrame:
    """Read an AEC results CSV.

    Every results CSV carries a one-line provenance preamble above the header row.

    A handful of rows are malformed at the source: the 2023 referendum polling place
    file ships two rows with a surplus empty address field (16 fields against a
    15-field header). Those rows are repaired by dropping one empty field rather than
    discarded, and the repair is reported.
    """
    if not path.exists():
        return pd.DataFrame()
    read_kwargs = {
        "skiprows": 1,
        "dtype": str,
        "keep_default_na": False,
        "na_values": [""],
    }
    try:
        return pd.read_csv(path, **read_kwargs)
    except pd.errors.ParserError:
        pass

    with path.open(newline="", encoding="utf-8", errors="replace") as fh:
        reader = csv.reader(fh)
        next(reader)
        width = len(next(reader))

    repaired: int = 0

    def repair(row: list[str]) -> list[str] | None:
        nonlocal repaired
        out = list(row)
        while len(out) > width and "" in out:
            out.remove("")
        if len(out) != width:
            return None
        repaired += 1
        return out

    df = pd.read_csv(path, **read_kwargs, engine="python", on_bad_lines=repair)
    if repaired:
        print(
            f"  NOTE {path.name}: repaired {repaired} malformed row(s) from the source"
        )
    return df


def read_transparency_csv(path: Path) -> pd.DataFrame:
    """Read a Transparency Register CSV (no preamble, fully quoted)."""
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path, dtype=str, keep_default_na=False, na_values=[""])


def conform(
    df: pd.DataFrame, table: str, rename: dict[str, str]
) -> pd.DataFrame:
    """Rename source columns and reindex onto the architecture's column list.

    Selecting by name (never by position) is what makes this robust to the two known
    drifts in the AEC files: the two-party-preferred column order flips between
    elections, and 2004 ships ``SittingMemberFl`` where later years ship
    ``Elected``/``HistoricElected``. Columns absent from a given vintage arrive as NA.
    """
    if df.empty:
        return pd.DataFrame(columns=schema.column_names(table))
    out = df.rename(columns=rename)
    # Drop source columns that map nowhere, then add missing architecture columns.
    keep = [c for c in out.columns if c in schema.column_names(table)]
    out = out.loc[:, ~out.columns.duplicated()][keep]
    return out.reindex(columns=schema.column_names(table))


def _events_all() -> dict[int, tuple[int, str, str, str | None, str | None]]:
    """Every event, general and otherwise, keyed by AEC event id."""
    out: dict[int, tuple[int, str, str, str | None, str | None]] = {}
    for ev, (year, _seg) in constants.FEDERAL_ELECTIONS.value.items():
        out[ev] = (
            year,
            f"{year} federal election",
            "federal_election",
            None,
            None,
        )
    for ev, meta in constants.EXTRA_EVENTS.value.items():
        out[ev] = meta
    return out


def _event_dir(event_id: int, root: Path) -> Path:
    if event_id in constants.FEDERAL_ELECTIONS.value:
        year, _ = constants.FEDERAL_ELECTIONS.value[event_id]
        return root / "input" / "results" / str(year)
    return root / "input" / "results_extra" / str(event_id)


def _stamp(df: pd.DataFrame, event_id: int) -> pd.DataFrame:
    """Prepend the event keys that the source CSVs do not carry."""
    if df.empty:
        return df
    year = _events_all()[event_id][0]
    df = df.copy()
    # `conform` already reindexed these in as empty columns; fill them, don't insert.
    df["election_id"] = str(event_id)
    df["year"] = year
    return df


# ======================================================================================
# Rename maps
# ======================================================================================

R_POLLING_PLACE = {
    "State": "state_abbreviation",
    "StateAb": "state_abbreviation",
    "DivisionID": "division_id",
    "DivisionNm": "division_name",
    "PollingPlaceID": "polling_place_id",
    "PollingPlaceTypeID": "polling_place_type_id",
    "PollingPlaceNm": "polling_place_name",
    "PremisesNm": "premises_name",
    "PremisesAddress1": "premises_address_1",
    "PremisesAddress2": "premises_address_2",
    "PremisesAddress3": "premises_address_3",
    "PremisesSuburb": "premises_suburb",
    "PremisesStateAb": "premises_state_abbreviation",
    "PremisesPostCode": "premises_postcode",
    "Latitude": "latitude",
    "Longitude": "longitude",
}

R_PARTY = {
    "StateAb": "state_abbreviation",
    "PartyAb": "party_abbreviation",
    "RegisteredPartyAb": "registered_party_abbreviation",
    "PartyNm": "party_name",
}

_R_CANDIDATE_CORE = {
    "StateAb": "state_abbreviation",
    "DivisionID": "division_id",
    "DivisionNm": "division_name",
    "CandidateID": "candidate_id",
    "Surname": "surname",
    "GivenNm": "given_name",
    "PartyAb": "party_abbreviation",
    "PartyNm": "party_name",
    "Elected": "elected",
    "HistoricElected": "historic_elected",
    "SittingMemberFl": "sitting_member",
    "BallotPosition": "ballot_position",
}

R_HOUSE_CANDIDATE = dict(_R_CANDIDATE_CORE)

R_HOUSE_FP_DIVISION = {
    **_R_CANDIDATE_CORE,
    "OrdinaryVotes": "ordinary_votes",
    "AbsentVotes": "absent_votes",
    "ProvisionalVotes": "provisional_votes",
    "PrePollVotes": "pre_poll_votes",
    "PostalVotes": "postal_votes",
    "TotalVotes": "total_votes",
    "Swing": "swing",
}

R_HOUSE_PP = {
    **_R_CANDIDATE_CORE,
    "PollingPlaceID": "polling_place_id",
    "PollingPlace": "polling_place_name",
    "OrdinaryVotes": "ordinary_votes",
    "Swing": "swing",
}

R_TPP = {
    "StateAb": "state_abbreviation",
    "DivisionID": "division_id",
    "DivisionNm": "division_name",
    "PartyAb": "party_abbreviation",
    "PollingPlaceID": "polling_place_id",
    "PollingPlace": "polling_place_name",
    "Australian Labor Party Votes": "labor_votes",
    "Australian Labor Party Percentage": "labor_percentage",
    "Liberal/National Coalition Votes": "coalition_votes",
    "Liberal/National Coalition Percentage": "coalition_percentage",
    "TotalVotes": "total_votes",
    "Swing": "swing",
}

R_SENATE_CANDIDATE = {
    "StateAb": "state_abbreviation",
    "CandidateID": "candidate_id",
    "Surname": "surname",
    "GivenNm": "given_name",
    "PartyAb": "party_abbreviation",
    "PartyNm": "party_name",
    "Elected": "elected",
    "HistoricElected": "historic_elected",
    "SittingMemberFl": "sitting_member",
    "ElectedOrder": "elected_order",
}

R_SENATE_FP = {
    "StateAb": "state_abbreviation",
    "DivisionID": "division_id",
    "DivisionNm": "division_name",
    "Group": "group_abbreviation",
    "Ticket": "group_abbreviation",
    "BallotPosition": "ballot_position",
    "CandidateID": "candidate_id",
    "CandidateDetails": "candidate_details",
    "PartyName": "party_name",
    "PartyAb": "party_abbreviation",
    "Elected": "elected",
    "HistoricElected": "historic_elected",
    "OrdinaryVotes": "ordinary_votes",
    "AbsentVotes": "absent_votes",
    "ProvisionalVotes": "provisional_votes",
    "PrePollVotes": "pre_poll_votes",
    "PostalVotes": "postal_votes",
    "TotalVotes": "total_votes",
}

R_REFERENDUM_PP = {
    "QuestionNo": "question_number",
    "StateAb": "state_abbreviation",
    "DivisionID": "division_id",
    "DivisionName": "division_name",
    "PollingPlaceId": "polling_place_id",
    "PollingPlaceNm": "polling_place_name",
    "YesVotes": "yes_votes",
    "YesPercentage": "yes_percentage",
    "NoVotes": "no_votes",
    "NoPercentage": "no_percentage",
    "FormalVotes": "formal_votes",
    "FormalPercentage": "formal_percentage",
    "InformalVotes": "informal_votes",
    "InformalPercentage": "informal_percentage",
    "TotalVotes": "total_votes",
}

R_SUMMARY = {
    "StateAb": "state_abbreviation",
    "DivisionID": "division_id",
    "DivisionNm": "division_name",
    "Enrolment": "enrolment",
    "Turnout": "turnout",
    "TurnoutPercentage": "turnout_percentage",
    "TurnoutSwing": "turnout_swing",
    "FormalVotes": "formal_votes",
    "InformalVotes": "informal_votes",
    "InformalPercent": "informal_percentage",
    "InformalSwing": "informal_swing",
    "OrdinaryVotes": "ordinary_votes",
    "AbsentVotes": "absent_votes",
    "ProvisionalVotes": "provisional_votes",
    "PrePollVotes": "pre_poll_votes",
    "PostalVotes": "postal_votes",
    "TotalVotes": "total_votes",
}


# ======================================================================================
# Table builders — results
# ======================================================================================


def build_election() -> pd.DataFrame:
    rows = []
    for ev, (year, name, etype, division, state) in sorted(
        _events_all().items(), key=lambda kv: (kv[1][0], kv[0])
    ):
        rows.append(
            {
                "year": year,
                "election_id": str(ev),
                "election_name": name,
                "election_type": etype,
                "division_name": division,
                "state_abbreviation": state,
            }
        )
    return pd.DataFrame(rows, columns=schema.column_names("election"))


def _concat_simple(
    table: str,
    filename: str,
    rename: dict[str, str],
    root: Path,
    events: list[int],
) -> pd.DataFrame:
    frames = []
    for ev in events:
        path = _event_dir(ev, root) / f"{filename}-{ev}.csv"
        df = conform(read_aec_csv(path), table, rename)
        frames.append(_stamp(df, ev))
    frames = [f for f in frames if not f.empty]
    if not frames:
        return pd.DataFrame(columns=schema.column_names(table))
    return pd.concat(frames, ignore_index=True)[schema.column_names(table)]


def _concat_by_state(
    table: str,
    filename: str,
    rename: dict[str, str],
    root: Path,
    events: list[int],
) -> pd.DataFrame:
    frames = []
    for ev in events:
        event_dir = _event_dir(ev, root)
        for path in sorted(event_dir.glob(f"{filename}-{ev}-*.csv")):
            df = conform(read_aec_csv(path), table, rename)
            frames.append(_stamp(df, ev))
    frames = [f for f in frames if not f.empty]
    if not frames:
        return pd.DataFrame(columns=schema.column_names(table))
    return pd.concat(frames, ignore_index=True)[schema.column_names(table)]


def _all_events() -> list[int]:
    return sorted(_events_all())


def _general_events() -> list[int]:
    return sorted(constants.FEDERAL_ELECTIONS.value)


def build_polling_place(root: Path) -> pd.DataFrame:
    return _concat_simple(
        "polling_place",
        "GeneralPollingPlacesDownload",
        R_POLLING_PLACE,
        root,
        _all_events(),
    )


def build_party(root: Path) -> pd.DataFrame:
    return _concat_simple(
        "party", "GeneralPartyDetailsDownload", R_PARTY, root, _all_events()
    )


def build_house_candidate(root: Path) -> pd.DataFrame:
    return _concat_simple(
        "house_candidate",
        "HouseCandidatesDownload",
        R_HOUSE_CANDIDATE,
        root,
        _all_events(),
    )


def build_house_first_preference_division(root: Path) -> pd.DataFrame:
    return _concat_simple(
        "house_first_preference_division",
        "HouseFirstPrefsByCandidateByVoteTypeDownload",
        R_HOUSE_FP_DIVISION,
        root,
        _general_events(),
    )


def build_house_first_preference_polling_place(root: Path) -> pd.DataFrame:
    return _concat_by_state(
        "house_first_preference_polling_place",
        "HouseStateFirstPrefsByPollingPlaceDownload",
        R_HOUSE_PP,
        root,
        _all_events(),
    )


def build_house_tcp_polling_place(root: Path) -> pd.DataFrame:
    return _concat_simple(
        "house_two_candidate_preferred_polling_place",
        "HouseTcpByCandidateByPollingPlaceDownload",
        R_HOUSE_PP,
        root,
        _all_events(),
    )


def build_house_tpp_division(root: Path) -> pd.DataFrame:
    return _concat_simple(
        "house_two_party_preferred_division",
        "HouseTppByDivisionDownload",
        R_TPP,
        root,
        _general_events(),
    )


def build_house_tpp_polling_place(root: Path) -> pd.DataFrame:
    return _concat_simple(
        "house_two_party_preferred_polling_place",
        "HouseTppByPollingPlaceDownload",
        R_TPP,
        root,
        _all_events(),
    )


def build_senate_candidate(root: Path) -> pd.DataFrame:
    """Senate nominations, with the elected order joined on from the elected file."""
    table = "senate_candidate"
    frames = []
    for ev in _all_events():
        event_dir = _event_dir(ev, root)
        cand = conform(
            read_aec_csv(event_dir / f"SenateCandidatesDownload-{ev}.csv"),
            table,
            R_SENATE_CANDIDATE,
        )
        if cand.empty:
            continue
        elected_raw = read_aec_csv(
            event_dir / f"SenateSenatorsElectedDownload-{ev}.csv"
        )
        if not elected_raw.empty and "ElectedOrder" in elected_raw.columns:
            key = ["state_abbreviation", "surname", "given_name"]
            el = elected_raw.rename(columns=R_SENATE_CANDIDATE)[
                [*key, "elected_order"]
            ]
            el = el.drop_duplicates(subset=key)
            cand = cand.drop(columns=["elected_order"]).merge(
                el, on=key, how="left"
            )
            cand = cand.reindex(columns=schema.column_names(table))
        frames.append(_stamp(cand, ev))
    frames = [f for f in frames if not f.empty]
    if not frames:
        return pd.DataFrame(columns=schema.column_names(table))
    return pd.concat(frames, ignore_index=True)[schema.column_names(table)]


def build_senate_first_preference_division(root: Path) -> pd.DataFrame:
    return _concat_simple(
        "senate_first_preference_division",
        "SenateFirstPrefsByDivisionByVoteTypeDownload",
        R_SENATE_FP,
        root,
        _all_events(),
    )


def build_division_summary(root: Path) -> pd.DataFrame:
    """Enrolment, turnout, informal votes and vote types, per division and chamber."""
    table = "division_summary"
    cols = schema.column_names(table)
    chambers = {
        "house": (
            "HouseTurnoutByDivisionDownload",
            "HouseInformalByDivisionDownload",
            "HouseVotesCountedByDivisionDownload",
        ),
        "senate": (
            "SenateTurnoutByDivisionDownload",
            "SenateInformalByDivisionDownload",
            "SenateVotesCountedByDivisionDownload",
        ),
        "referendum": (
            "ReferendumTurnoutByDivisionDownload",
            "ReferendumInformalByDivisionDownload",
            "ReferendumVotesCountedByDivisionDownload",
        ),
    }
    frames = []
    for ev in _all_events():
        event_dir = _event_dir(ev, root)
        for chamber, (turnout_f, informal_f, votes_f) in chambers.items():
            turnout = read_aec_csv(event_dir / f"{turnout_f}-{ev}.csv")
            if turnout.empty:
                continue
            base = turnout.rename(columns=R_SUMMARY)
            for fname, extra in (
                (
                    informal_f,
                    (
                        "formal_votes",
                        "informal_votes",
                        "informal_percentage",
                        "informal_swing",
                        "total_votes",
                    ),
                ),
                (
                    votes_f,
                    (
                        "ordinary_votes",
                        "absent_votes",
                        "provisional_votes",
                        "pre_poll_votes",
                        "postal_votes",
                        "total_votes",
                    ),
                ),
            ):
                other = read_aec_csv(event_dir / f"{fname}-{ev}.csv")
                if other.empty:
                    continue
                other = other.rename(columns=R_SUMMARY)
                take = [
                    c
                    for c in extra
                    if c in other.columns and c not in base.columns
                ]
                if not take:
                    continue
                base = base.merge(
                    other[["division_id", *take]].drop_duplicates(
                        subset=["division_id"]
                    ),
                    on="division_id",
                    how="left",
                )
            base["chamber"] = chamber
            frames.append(_stamp(base.reindex(columns=cols), ev))
    frames = [f for f in frames if not f.empty]
    if not frames:
        return pd.DataFrame(columns=cols)
    return pd.concat(frames, ignore_index=True)[cols]


def build_referendum_polling_place(root: Path) -> pd.DataFrame:
    return _concat_by_state(
        "referendum_polling_place",
        "ReferendumPollingPlaceResultsByStateDownload",
        R_REFERENDUM_PP,
        root,
        _all_events(),
    )


# ======================================================================================
# Table builders — Transparency Register
# ======================================================================================


def _fy_start_year(fy: str | float | None) -> float | None:
    """'2024-25' or '1998-1999' -> 1998/2024. The AEC changed format at 2011-12."""
    if not isinstance(fy, str):
        return None
    m = re.match(r"^\s*(\d{4})", fy)
    return int(m.group(1)) if m else None


def _event_year(event: str | float | None) -> float | None:
    if not isinstance(event, str):
        return None
    m = re.match(r"^\s*(\d{4})", event.strip())
    if m:
        return int(m.group(1))
    return TRANSPARENCY_EVENT_YEARS.get(event.strip().lower())


def _tidy(series: pd.Series) -> pd.Series:
    """Transparency CSVs pad some name fields with leading spaces."""
    stripped = series.astype("string").str.strip()
    return stripped.mask(stripped == "")


def _transparency_dir(root: Path, bundle: str) -> Path:
    return root / "input" / "transparency" / bundle


# (file, donor column, recipient column, date column, value column, direction, return_type)
_DONATION_SOURCES = [
    (
        "AllAnnualData",
        "Donations Made.csv",
        "Donor Name",
        "Donation Made To",
        "Date",
        "Value",
        "made",
        "Donor Return",
    ),
    (
        "AllAnnualData",
        "Donor Donations Received.csv",
        "Donation Received From",
        "Name",
        "Date",
        "Value",
        "received",
        "Donor Return",
    ),
    (
        "AllAnnualData",
        "Third Party Donations Received.csv",
        "Donation Received From",
        "Name",
        "Date",
        "Value",
        "received",
        "Third Party Return",
    ),
    (
        "AllElectionsData",
        "Donor Donations Made.csv",
        "Donor Name",
        "Donated To",
        "Donated To Date Of Gift",
        "Donated To Gift Value",
        "made",
        "Donor Return",
    ),
    (
        "AllElectionsData",
        "Donor Donations Received.csv",
        "Gift From Name",
        "Donor Name",
        "Gift From Date Of Gift",
        "Gift From Gift Value",
        "received",
        "Donor Return",
    ),
    (
        "AllElectionsData",
        "Senate Groups and Candidate Donations.csv",
        "Donor Name",
        "Name",
        "Date Of Gift",
        "Gift Value",
        "received",
        None,
    ),
    (
        "AllElectionsData",
        "Third Party Return Donations Made.csv",
        "Third Party Name",
        "Name",
        "Date Of Donation",
        "Donation Value",
        "made",
        "Third Party Return",
    ),
    (
        "AllElectionsData",
        "Third Party Return Donations Received.csv",
        "Donor Name",
        "Third Party Name",
        "Date Of Gift",
        "Gift Value",
        "received",
        "Third Party Return",
    ),
    (
        "AllReferendumData",
        "Referendum Donations Made.csv",
        "Donor Name",
        "Donated to name",
        "Date",
        "Value",
        "made",
        "Referendum Donor Return",
    ),
    (
        "AllReferendumData",
        "Referendum Entity Donations Received.csv",
        "Donor name",
        "Name",
        "Date",
        "Value",
        "received",
        "Referendum Entity Return",
    ),
]

_BUNDLE_TO_DISCLOSURE = {
    "AllAnnualData": "annual",
    "AllElectionsData": "election",
    "AllReferendumData": "referendum",
}


def build_disclosure_donation(root: Path) -> pd.DataFrame:
    table = "disclosure_donation"
    cols = schema.column_names(table)
    frames = []
    for (
        bundle,
        fname,
        donor_c,
        recip_c,
        date_c,
        value_c,
        direction,
        rtype,
    ) in _DONATION_SOURCES:
        raw = read_transparency_csv(_transparency_dir(root, bundle) / fname)
        if raw.empty:
            continue
        out = pd.DataFrame(index=raw.index)
        out["disclosure_type"] = _BUNDLE_TO_DISCLOSURE[bundle]
        if "Financial Year" in raw.columns:
            out["financial_year"] = _tidy(raw["Financial Year"])
            out["election_name"] = None
            out["year"] = out["financial_year"].map(_fy_start_year)
        else:
            out["financial_year"] = None
            out["election_name"] = _tidy(raw["Event"])
            out["year"] = out["election_name"].map(_event_year)
        if rtype is None:
            out["return_type"] = _tidy(
                raw["Return Type (Candidate/Senate Group)"]
            )
        else:
            out["return_type"] = rtype
        out["direction"] = direction
        out["donor_name"] = _tidy(raw[donor_c])
        out["recipient_name"] = _tidy(raw[recip_c])
        out["donation_date"] = pd.to_datetime(
            raw[date_c], format="%d/%m/%Y", errors="coerce"
        ).dt.date
        out["value"] = pd.to_numeric(raw[value_c], errors="coerce")
        frames.append(out.reindex(columns=cols))
    if not frames:
        return pd.DataFrame(columns=cols)
    return pd.concat(frames, ignore_index=True)[cols]


def build_disclosure_receipt(root: Path) -> pd.DataFrame:
    table = "disclosure_receipt"
    cols = schema.column_names(table)
    raw = read_transparency_csv(
        _transparency_dir(root, "AllAnnualData") / "Detailed Receipts.csv"
    )
    if raw.empty:
        return pd.DataFrame(columns=cols)
    out = pd.DataFrame(index=raw.index)
    out["financial_year"] = _tidy(raw["Financial Year"])
    out["year"] = out["financial_year"].map(_fy_start_year)
    out["return_type"] = _tidy(raw["Return Type"])
    out["recipient_name"] = _tidy(raw["Recipient Name"])
    out["received_from"] = _tidy(raw["Received From"])
    out["receipt_type"] = _tidy(raw["Receipt Type"])
    out["value"] = pd.to_numeric(raw["Value"], errors="coerce")
    return out.reindex(columns=cols)


# (file, fixed return type or None to read the column, extra column renames)
_ANNUAL_RETURN_SOURCES = [
    ("Party Returns.csv", "Political Party Return", {}),
    (
        "Associated Entity Returns.csv",
        "Associated Entity Return",
        {"Discretionary Benefits": "total_discretionary_benefits"},
    ),
    ("Significant Third Party Returns.csv", None, {}),
    (
        "Third Party Returns.csv",
        "Third Party Return",
        {"Total Gifts Received": "total_donations_received"},
    ),
    ("Donor Returns.csv", "Donor Return", {}),
    ("MemberOfParliamentReturns.csv", None, {}),
]

_ANNUAL_RETURN_RENAME = {
    "Name": "name",
    "Lodged on behalf of": "lodged_on_behalf_of",
    "Party Group": "party_group",
    "AssociatedParties": "associated_parties",
    "ClientType": "client_type",
    "ClientFileId": "client_file_id",
    "ABN": "abn",
    "ACN": "acn",
    "Total Receipts": "total_receipts",
    "Total Payments": "total_payments",
    "Total Debts": "total_debts",
    "Total Discretionary Benefits": "total_discretionary_benefits",
    "Capital Contributions": "capital_contributions",
    "Total Donations Made": "total_donations_made",
    "Total Donations Received": "total_donations_received",
    "Total Expenditure": "total_expenditure",
    "Electoral Expenditure": "electoral_expenditure",
    "Number of Donors": "number_of_donors",
}


def build_disclosure_return_annual(root: Path) -> pd.DataFrame:
    table = "disclosure_return_annual"
    cols = schema.column_names(table)
    numeric = {
        c.name
        for c in schema.TABLES[table]
        if c.bigquery_type in ("FLOAT64", "INT64")
    } - {"year"}
    frames = []
    for fname, rtype, extra in _ANNUAL_RETURN_SOURCES:
        raw = read_transparency_csv(
            _transparency_dir(root, "AllAnnualData") / fname
        )
        if raw.empty:
            continue
        out = raw.rename(columns={**_ANNUAL_RETURN_RENAME, **extra})
        out["financial_year"] = _tidy(raw["Financial Year"])
        out["year"] = out["financial_year"].map(_fy_start_year)
        out["return_type"] = (
            _tidy(raw["Return Type"]) if rtype is None else rtype
        )
        out = out.loc[:, ~out.columns.duplicated()].reindex(columns=cols)
        for col in numeric:
            out[col] = pd.to_numeric(out[col], errors="coerce")
        for col in cols:
            if col not in numeric and col != "year":
                out[col] = _tidy(out[col])
        frames.append(out)
    if not frames:
        return pd.DataFrame(columns=cols)
    return pd.concat(frames, ignore_index=True)[cols]


_ELECTION_RETURN_RENAME = {
    "Return Type (Candidate/Senate Group)": "return_type",
    "Name": "name",
    "Party ID": "party_id",
    "Party Name": "party_name",
    "Electorate Name": "electorate_name",
    "Electorate State": "electorate_state",
    "Nil Return": "nil_return",
    "Amendment No": "amendment_number",
    "Total Gift Value": "total_gift_value",
    "Number Of Donors": "number_of_donors",
    "Total Electoral Expenditure": "total_electoral_expenditure",
    "Discretionary Benefits Received": "discretionary_benefits_received",
    "Broadcasting Cost": "broadcasting_cost",
    "Publishing Cost": "publishing_cost",
    "Display Ad Cost": "display_ad_cost",
    "Direct Mailing": "direct_mailing_cost",
    "Campaign Material Costs": "campaign_material_cost",
    "Opinion Polls": "opinion_poll_cost",
}


def build_disclosure_election_return(root: Path) -> pd.DataFrame:
    """Candidate and Senate-group return summaries, with the expense breakdown joined on."""
    table = "disclosure_election_return"
    cols = schema.column_names(table)
    base_dir = _transparency_dir(root, "AllElectionsData")
    summary = read_transparency_csv(
        base_dir / "Senate Groups and Candidate Return Summary.csv"
    )
    if summary.empty:
        return pd.DataFrame(columns=cols)
    out = summary.rename(columns=_ELECTION_RETURN_RENAME)
    out["election_name"] = _tidy(summary["Event"])
    out["year"] = out["election_name"].map(_event_year)

    expenses = read_transparency_csv(
        base_dir / "Senate Groups and Candidate Expenses.csv"
    )
    if not expenses.empty:
        exp = expenses.rename(columns=_ELECTION_RETURN_RENAME)
        exp["election_name"] = _tidy(expenses["Event"])
        breakdown = [
            "broadcasting_cost",
            "publishing_cost",
            "display_ad_cost",
            "direct_mailing_cost",
            "campaign_material_cost",
            "opinion_poll_cost",
        ]
        key = ["election_name", "return_type", "name"]
        exp = exp[key + breakdown].drop_duplicates(subset=key)
        out = out.merge(exp, on=key, how="left")

    out = out.loc[:, ~out.columns.duplicated()].reindex(columns=cols)
    numeric = {
        c.name
        for c in schema.TABLES[table]
        if c.bigquery_type in ("FLOAT64", "INT64")
    } - {"year"}
    for col in cols:
        if col in numeric:
            out[col] = pd.to_numeric(out[col], errors="coerce")
        elif col != "year":
            out[col] = _tidy(out[col])
    return out[cols]


# ======================================================================================
# Dictionary
# ======================================================================================

_DICTIONARY_ENTRIES: list[tuple[str, str, str, str]] = [
    ("party", "state_abbreviation", "NAT", "Partido de registro nacional"),
    ("election", "election_type", "federal_election", "Eleição geral federal"),
    (
        "election",
        "election_type",
        "by_election",
        "Eleição suplementar de uma divisão",
    ),
    (
        "election",
        "election_type",
        "senate_election",
        "Eleição de Senado restrita a um estado",
    ),
    ("election", "election_type", "referendum", "Referendo nacional"),
    (
        "division_summary",
        "chamber",
        "house",
        "Apuração da Câmara dos Representantes",
    ),
    ("division_summary", "chamber", "senate", "Apuração do Senado"),
    ("division_summary", "chamber", "referendum", "Apuração do referendo"),
    (
        "disclosure_donation",
        "disclosure_type",
        "annual",
        "Declaração anual do Transparency Register",
    ),
    (
        "disclosure_donation",
        "disclosure_type",
        "election",
        "Declaração referente a um evento eleitoral",
    ),
    (
        "disclosure_donation",
        "disclosure_type",
        "referendum",
        "Declaração referente a um referendo",
    ),
    (
        "disclosure_donation",
        "direction",
        "made",
        "Doação declarada por quem doou",
    ),
    (
        "disclosure_donation",
        "direction",
        "received",
        "Doação declarada por quem recebeu",
    ),
]

_YES_NO_COLUMNS = [
    ("house_candidate", "elected"),
    ("house_candidate", "historic_elected"),
    ("house_candidate", "sitting_member"),
    ("house_first_preference_division", "elected"),
    ("house_first_preference_division", "historic_elected"),
    ("house_first_preference_division", "sitting_member"),
    ("house_first_preference_polling_place", "elected"),
    ("house_first_preference_polling_place", "historic_elected"),
    ("house_first_preference_polling_place", "sitting_member"),
    ("house_two_candidate_preferred_polling_place", "elected"),
    ("house_two_candidate_preferred_polling_place", "historic_elected"),
    ("house_two_candidate_preferred_polling_place", "sitting_member"),
    ("senate_candidate", "elected"),
    ("senate_candidate", "historic_elected"),
    ("senate_candidate", "sitting_member"),
    ("senate_first_preference_division", "elected"),
    ("senate_first_preference_division", "historic_elected"),
    ("disclosure_election_return", "nil_return"),
]


def build_dicionario(
    root: Path, tables: dict[str, pd.DataFrame]
) -> pd.DataFrame:
    """Dictionary rows for every column flagged covered_by_dictionary.

    Entries for free-form coded columns (polling place type, receipt type, return type)
    are enumerated from the cleaned data so the dictionary cannot drift from it.
    """
    rows = [
        {
            "id_tabela": t,
            "nome_coluna": c,
            "chave": k,
            "cobertura_temporal": "",
            "valor": v,
        }
        for t, c, k, v in _DICTIONARY_ENTRIES
    ]
    for table, column in _YES_NO_COLUMNS:
        for key, value in (("Y", "Sim"), ("N", "Não")):
            rows.append(
                {
                    "id_tabela": table,
                    "nome_coluna": column,
                    "chave": key,
                    "cobertura_temporal": "",
                    "valor": value,
                }
            )
    # Enumerated coded values, taken from the cleaned tables themselves.
    enumerated = [
        ("polling_place", "polling_place_type_id"),
        ("disclosure_receipt", "receipt_type"),
        ("disclosure_receipt", "return_type"),
        ("disclosure_donation", "return_type"),
        ("disclosure_return_annual", "return_type"),
        ("disclosure_return_annual", "client_type"),
        ("disclosure_election_return", "return_type"),
    ]
    for table, column in enumerated:
        df = tables.get(table)
        if df is None or df.empty or column not in df.columns:
            continue
        for key in sorted(pd.Series(df[column]).dropna().astype(str).unique()):
            rows.append(
                {
                    "id_tabela": table,
                    "nome_coluna": column,
                    "chave": key,
                    "cobertura_temporal": "",
                    "valor": key,
                }
            )
    return pd.DataFrame(rows, columns=schema.column_names("dicionario"))


# ======================================================================================
# Parquet output
# ======================================================================================

_ARROW = {
    "STRING": pa.string(),
    "INT64": pa.int64(),
    "FLOAT64": pa.float64(),
    "DATE": pa.date32(),
}


def to_string_table(df: pd.DataFrame, table: str) -> pa.Table:
    """Coerce to the architecture's real types, then cast every column to STRING.

    Staging is all-STRING by house convention and the dbt model ``safe_cast``s each
    column back. Going through the real type first matters: a bare ``astype(str)``
    renders NULL as the literal ``"nan"`` (which ``safe_cast`` will not turn back into
    NULL) and an integer read as float as ``"2004.0"``.
    """
    types = schema.column_types(table)
    arrays = []
    for name in schema.column_names(table):
        series = (
            df[name] if name in df.columns else pd.Series([None] * len(df))
        )
        target = _ARROW[types[name]]
        if target == pa.int64():
            series = (
                pd.to_numeric(series, errors="coerce").round().astype("Int64")
            )
        elif target == pa.float64():
            series = pd.to_numeric(series, errors="coerce")
        elif target == pa.date32():
            series = pd.to_datetime(series, errors="coerce").dt.date
        else:
            series = series.astype("string")
        arr = pa.array(series, type=target, from_pandas=True)
        arrays.append(arr.cast(pa.string()))
    return pa.Table.from_arrays(
        arrays,
        schema=pa.schema(
            [(n, pa.string()) for n in schema.column_names(table)]
        ),
    )


def write_partitioned(df: pd.DataFrame, table: str, output_dir: Path) -> int:
    """Write one table as hive-partitioned, all-STRING, snappy parquet."""
    out = output_dir / table
    if out.exists():
        shutil.rmtree(out)
    out.mkdir(parents=True, exist_ok=True)

    partition = schema.PARTITION_COLUMNS[table]
    if not partition:
        pq.write_table(
            to_string_table(df, table),
            out / "data.parquet",
            compression="snappy",
        )
        return len(df)

    written = 0
    years = pd.to_numeric(df["year"], errors="coerce")
    for year in sorted(years.dropna().unique()):
        chunk = df[years == year]
        if chunk.empty:
            continue
        part = out / f"year={int(year)}"
        part.mkdir(parents=True, exist_ok=True)
        # The partition key is carried by the directory name, not the file.
        pq.write_table(
            to_string_table(chunk, table).drop_columns(["year"]),
            part / "data.parquet",
            compression="snappy",
        )
        written += len(chunk)
    dropped = years.isna().sum()
    if dropped:
        print(f"  WARNING {table}: {dropped} rows dropped for a missing year")
    return written


BUILDERS = {
    "polling_place": build_polling_place,
    "party": build_party,
    "house_candidate": build_house_candidate,
    "house_first_preference_division": build_house_first_preference_division,
    "house_first_preference_polling_place": build_house_first_preference_polling_place,
    "house_two_candidate_preferred_polling_place": build_house_tcp_polling_place,
    "house_two_party_preferred_division": build_house_tpp_division,
    "house_two_party_preferred_polling_place": build_house_tpp_polling_place,
    "senate_candidate": build_senate_candidate,
    "senate_first_preference_division": build_senate_first_preference_division,
    "division_summary": build_division_summary,
    "referendum_polling_place": build_referendum_polling_place,
    "disclosure_donation": build_disclosure_donation,
    "disclosure_receipt": build_disclosure_receipt,
    "disclosure_return_annual": build_disclosure_return_annual,
    "disclosure_election_return": build_disclosure_election_return,
}


def clean_all(
    root: Path | None = None, output_dir: Path | None = None
) -> dict[str, int]:
    """Build every table and write partitioned parquet. Returns row counts."""
    root = root or data_root()
    output_dir = output_dir or (root / "output")
    output_dir.mkdir(parents=True, exist_ok=True)

    tables: dict[str, pd.DataFrame] = {"election": build_election()}
    for name, builder in BUILDERS.items():
        tables[name] = builder(root)
    tables["dicionario"] = build_dicionario(root, tables)

    counts: dict[str, int] = {}
    for name in constants.TABLES.value:
        counts[name] = write_partitioned(tables[name], name, output_dir)
    return counts
