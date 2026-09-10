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
