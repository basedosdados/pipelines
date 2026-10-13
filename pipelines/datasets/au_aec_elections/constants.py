"""Constants for au_aec_elections — AEC federal election results and disclosures."""

import os
from enum import Enum
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
ARCHITECTURE_DIR = (
    REPO_ROOT / "models" / "au_aec_elections" / "code" / "architecture"
)


def data_root() -> Path:
    """Scratch location for raw downloads and cleaned parquet (never inside the repo)."""
    return Path(
        os.environ.get(
            "AEC_DATA", Path.home() / "Downloads" / "au_aec_elections_data"
        )
    )


class constants(Enum):
    DATASET_ID = "au_aec_elections"

    RESULTS_BASE_URL = "https://results.aec.gov.au"
    TRANSPARENCY_BASE_URL = "https://transparency.aec.gov.au"

    STATES = ["NSW", "VIC", "QLD", "WA", "SA", "TAS", "ACT", "NT"]

    # Full federal elections: event_id -> (year, url path segment).
    # 2001 (event 10822) is excluded — the archive publishes no CSV downloads for it.
    FEDERAL_ELECTIONS = {
        12246: (2004, "results"),
        13745: (2007, "Website"),
        15508: (2010, "Website"),
        17496: (2013, "Website"),
        20499: (2016, "Website"),
        24310: (2019, "Website"),
        27966: (2022, "Website"),
        31496: (2025, "Website"),
    }

    # Non-general events. event_id -> (year, name, type, division_name, state).
    # 2005 Werriwa (event 12426) is excluded — no CSV downloads are published.
    EXTRA_EVENTS = {
        13813: (
            2008,
            "2008 Gippsland by-election",
            "by_election",
            "Gippsland",
            "VIC",
        ),
        13826: (2008, "2008 Mayo by-election", "by_election", "Mayo", "SA"),
        13827: (2008, "2008 Lyne by-election", "by_election", "Lyne", "NSW"),
        14357: (
            2009,
            "2009 Bradfield by-election",
            "by_election",
            "Bradfield",
            "NSW",
        ),
        14358: (
            2009,
            "2009 Higgins by-election",
            "by_election",
            "Higgins",
            "VIC",
        ),
        17552: (
            2014,
            "2014 Griffith by-election",
            "by_election",
            "Griffith",
            "QLD",
        ),
        17875: (
            2014,
            "2014 Western Australia Senate election",
            "senate_election",
            None,
            "WA",
        ),
        18126: (
            2015,
            "2015 Canning by-election",
            "by_election",
            "Canning",
            "WA",
        ),
        19402: (
            2015,
            "2015 North Sydney by-election",
            "by_election",
            "North Sydney",
            "NSW",
        ),
        21364: (
            2017,
            "2017 New England by-election",
            "by_election",
            "New England",
            "NSW",
        ),
        21379: (
            2017,
            "2017 Bennelong by-election",
            "by_election",
            "Bennelong",
            "NSW",
        ),
        21751: (
            2018,
            "2018 Batman by-election",
            "by_election",
            "Batman",
            "VIC",
        ),
        22692: (
            2018,
            "2018 Braddon by-election",
            "by_election",
            "Braddon",
            "TAS",
        ),
        22693: (
            2018,
            "2018 Fremantle by-election",
            "by_election",
            "Fremantle",
            "WA",
        ),
        22694: (
            2018,
            "2018 Longman by-election",
            "by_election",
            "Longman",
            "QLD",
        ),
        22695: (2018, "2018 Mayo by-election", "by_election", "Mayo", "SA"),
        22696: (2018, "2018 Perth by-election", "by_election", "Perth", "WA"),
        22844: (
            2018,
            "2018 Wentworth by-election",
            "by_election",
            "Wentworth",
            "NSW",
        ),
        25820: (
            2020,
            "2020 Eden-Monaro by-election",
            "by_election",
            "Eden-Monaro",
            "NSW",
        ),
        25881: (2020, "2020 Groom by-election", "by_election", "Groom", "QLD"),
        28791: (2023, "2023 Aston by-election", "by_election", "Aston", "VIC"),
        29422: (
            2023,
            "2023 Fadden by-election",
            "by_election",
            "Fadden",
            "QLD",
        ),
        29581: (2023, "2023 referendum", "referendum", None, None),
        29778: (
            2024,
            "2024 Dunkley by-election",
            "by_election",
            "Dunkley",
            "VIC",
        ),
        29807: (2024, "2024 Cook by-election", "by_election", "Cook", "NSW"),
        31633: (
            2026,
            "2026 Farrer by-election",
            "by_election",
            "Farrer",
            "NSW",
        ),
    }

    # Files fetched once per full federal election.
    NATIONAL_FILES = [
        "GeneralPollingPlacesDownload",
        "GeneralPartyDetailsDownload",
        "GeneralEnrolmentByDivisionDownload",
        "HouseCandidatesDownload",
        "HouseMembersElectedDownload",
        "HouseFirstPrefsByCandidateByVoteTypeDownload",
        "HouseTcpByCandidateByPollingPlaceDownload",
        "HouseTppByDivisionDownload",
        "HouseTppByPollingPlaceDownload",
        "HouseTurnoutByDivisionDownload",
        "HouseInformalByDivisionDownload",
        "HouseVotesCountedByDivisionDownload",
        "SenateCandidatesDownload",
        "SenateSenatorsElectedDownload",
        "SenateFirstPrefsByDivisionByVoteTypeDownload",
        "SenateTurnoutByDivisionDownload",
        "SenateInformalByDivisionDownload",
        "SenateVotesCountedByDivisionDownload",
    ]
    BY_STATE_FILES = ["HouseStateFirstPrefsByPollingPlaceDownload"]

    TRANSPARENCY_BUNDLES = [
        "AllAnnualData",
        "AllElectionsData",
        "AllReferendumData",
    ]

    USER_AGENT = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    )

    # Every table in the dataset, in publication order.
    TABLES = [
        "election",
        "polling_place",
        "party",
        "house_candidate",
        "house_first_preference_division",
        "house_first_preference_polling_place",
        "house_two_candidate_preferred_polling_place",
        "house_two_party_preferred_division",
        "house_two_party_preferred_polling_place",
        "senate_candidate",
        "senate_first_preference_division",
        "division_summary",
        "referendum_polling_place",
        "disclosure_donation",
        "disclosure_receipt",
        "disclosure_return_annual",
        "disclosure_election_return",
        "dicionario",
    ]
