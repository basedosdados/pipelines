"""Constants for au_tas_tec_elections — Tasmanian Electoral Commission results."""

import os
from enum import Enum
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
ARCHITECTURE_DIR = (
    REPO_ROOT / "models" / "au_tas_tec_elections" / "code" / "architecture"
)


def data_root() -> Path:
    """Scratch location for raw downloads and cleaned parquet (never inside the repo)."""
    return Path(
        os.environ.get(
            "TEC_DATA_ROOT",
            Path.home() / "Downloads" / "au_tas_tec_elections_data",
        )
    )


class constants(Enum):
    DATASET_ID = "au_tas_tec_elections"
    BACKEND_SLUG = "tas_elections"
    ORGANIZATION_SLUG = "au_tec"

    BASE_URL = "https://www.tec.tas.gov.au/"

    USER_AGENT = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"
    )

    # Section roots crawled to discover result pages and their file links. The TEC
    # moved the download path between the 2024 and 2025 House of Assembly elections,
    # so every URL is scraped from the rendered page and none is constructed.
    CRAWL_ROOTS = [
        "house-of-assembly/index.html",
        "house-of-assembly/elections-2025/index.html",
        "house-of-assembly/elections-2024/index.html",
        "house-of-assembly/StateElection2021/index.html",
        "house-of-assembly/StateElection2018/index.html",
        "house-of-assembly/StateElection2014/index.html",
        "legislative-council/index.html",
        "legislative-council/Previous_Elections/Index.html",
    ]

    # Every electoral event in the dataset: display name, chamber, polling day, and
    # the URL path that owns its results pages.
    #
    # The path prefix is load-bearing, not decoration. The 2021 House of Assembly and
    # the 2021 Legislative Council periodic elections were held on the same day and
    # their index pages cross-link each other, so following sibling links alone pulls
    # Derwent and Windermere into the House of Assembly event and Bass through Lyons
    # into the Legislative Council one. Contests are assigned by path, never by
    # reachability.
    ELECTION_META = {
        "hoa2018": (
            "2018 House of Assembly Election",
            "house_of_assembly",
            "state_general",
            "2018-03-03",
            "/house-of-assembly/StateElection2018/",
        ),
        "hoa2021": (
            "2021 House of Assembly Election",
            "house_of_assembly",
            "state_general",
            "2021-05-01",
            "/house-of-assembly/StateElection2021/",
        ),
        "hoa2024": (
            "2024 House of Assembly Election",
            "house_of_assembly",
            "state_general",
            "2024-03-23",
            "/house-of-assembly/elections-2024/",
        ),
        "hoa2025": (
            "2025 House of Assembly Election",
            "house_of_assembly",
            "state_general",
            "2025-07-19",
            "/house-of-assembly/elections-2025/",
        ),
        "lc2017pembroke": (
            "2017 Pembroke Legislative Council By-election",
            "legislative_council",
            "state_by_election",
            "2017-11-04",
            "/legislative-council/Previous_Elections/Pembroke2017/",
        ),
        "lc2018": (
            "2018 Legislative Council Elections",
            "legislative_council",
            "state_periodic",
            "2018-05-05",
            "/legislative-council/LegislativeCouncilElections_2018/",
        ),
        "lc2019": (
            "2019 Legislative Council Elections",
            "legislative_council",
            "state_periodic",
            "2019-05-04",
            "/legislative-council/LegislativeCouncilElections_2019/",
        ),
        "lc2020": (
            "2020 Legislative Council Elections",
            "legislative_council",
            "state_periodic",
            "2020-08-01",
            "/legislative-council/LegislativeCouncilElections_2020/",
        ),
        "lc2021": (
            "2021 Legislative Council Elections",
            "legislative_council",
            "state_periodic",
            "2021-05-01",
            "/legislative-council/LegislativeCouncilElections_2021/",
        ),
        "lc2022pembroke": (
            "2022 Pembroke Legislative Council By-election",
            "legislative_council",
            "state_by_election",
            "2022-09-10",
            "/legislative-council/legislative-council-byelection-2022/",
        ),
        "lc2022": (
            "2022 Legislative Council Elections",
            "legislative_council",
            "state_periodic",
            "2022-05-07",
            "/legislative-council/legislative-council-elections-2022/",
        ),
        "lc2023": (
            "2023 Legislative Council Elections",
            "legislative_council",
            "state_periodic",
            "2023-05-06",
            "/legislative-council/elections-2023/",
        ),
        "lc2024": (
            "2024 Legislative Council Elections",
            "legislative_council",
            "state_periodic",
            "2024-05-04",
            "/legislative-council/elections-2024/",
        ),
        "lc2025": (
            "2025 Legislative Council Elections",
            "legislative_council",
            "state_periodic",
            "2025-05-24",
            "/legislative-council/elections-2025/",
        ),
        "lc2026": (
            "2026 Legislative Council Elections",
            "legislative_council",
            "state_periodic",
            "2026-05-02",
            "/legislative-council/elections-2026/",
        ),
    }

    # Year index page each event's contests are discovered from.
    ELECTION_INDEX = {
        "hoa2018": "house-of-assembly/StateElection2018/Results/Results.html",
        "hoa2021": "house-of-assembly/StateElection2021/index.html",
        "hoa2024": "house-of-assembly/elections-2024/index.html",
        "hoa2025": "house-of-assembly/elections-2025/index.html",
        "lc2017pembroke": (
            "legislative-council/Previous_Elections/Pembroke2017/index.html"
        ),
        "lc2018": (
            "legislative-council/LegislativeCouncilElections_2018/Results/"
            "LCElection18Results.html"
        ),
        "lc2019": (
            "legislative-council/LegislativeCouncilElections_2019/Results/"
            "LCElection19Results.html"
        ),
        "lc2020": (
            "legislative-council/LegislativeCouncilElections_2020/Results/"
            "LCElection20Results.html"
        ),
        "lc2021": "legislative-council/LegislativeCouncilElections_2021/index.html",
        "lc2022": (
            "legislative-council/legislative-council-elections-2022/index.html"
        ),
        "lc2022pembroke": (
            "legislative-council/legislative-council-byelection-2022/index.html"
        ),
        "lc2023": "legislative-council/elections-2023/index.html",
        "lc2024": "legislative-council/elections-2024/index.html",
        "lc2025": "legislative-council/elections-2025/index.html",
        "lc2026": "legislative-council/elections-2026/index.html",
    }

    # Contests decided without a poll. The TEC publishes a results page for them
    # that carries no table at all, which is easily mistaken for a scrape failure.
    # Mersey 2021: "1 candidate (no ballot required) — GAFFNEY, Michael
    # (Independent) elected unopposed."
    UNOPPOSED = {
        ("lc2021", "mersey"): ("GAFFNEY, Michael", "Independent"),
    }

    # Two divisions went to the polls as by-elections on the same day as the
    # periodic elections around them, so their parent event is not uniformly one
    # type: Huon 2022 (Bastian Seidel's seat) and Elwick 2024 (Josh Willie's).
    CONCURRENT_BY_ELECTIONS = {("lc2022", "huon"), ("lc2024", "elwick")}

    # The House of Assembly grew from 25 seats to 35 at the 2024 election: five
    # divisions returning 5 members each became five returning 7. This is an era
    # break, not a continuity, and it changes the Hare-Clark quota.
    SEATS_PER_HOA_DIVISION = {2018: 5, 2021: 5, 2024: 7, 2025: 7}

    # House of Assembly divisions are coterminous with the five Commonwealth
    # divisions, per the TEC: "These divisions have the same boundaries as the five
    # Commonwealth House of Representatives divisions for Tasmania". Denison was
    # renamed Clark by amendments that gained Royal Assent on 28 September 2018, so
    # the 2018 election is fought as Denison and maps to the same seat.
    HOA_DIVISION_TO_CED = {
        "bass": "601",
        "braddon": "602",
        "clark": "603",
        "denison": "603",
        "franklin": "604",
        "lyons": "605",
    }

    TABLES = [
        "election",
        "candidate",
        "enrolment_turnout",
        "result_district",
        "result_voting_centre",
        "distribution_of_preferences",
        "voting_centre",
        "dicionario",
    ]

    PARTITIONED_BY_YEAR = [t for t in TABLES if t != "dicionario"]
