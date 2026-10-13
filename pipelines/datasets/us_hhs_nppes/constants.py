"""Constants for the us_hhs_nppes (NPPES / NPI registry) pipeline."""

from enum import Enum
from pathlib import Path

# repo root: pipelines/datasets/us_hhs_nppes/constants.py -> up 4
REPO_ROOT = Path(__file__).resolve().parents[3]


class constants(Enum):
    DATASET_ID = "us_hhs_nppes"

    # The listing page is plain HTML; the monthly link is discovered from it so
    # the pipeline never hardcodes a month.
    LISTING_URL = "https://download.cms.gov/nppes/NPI_Files.html"
    DOWNLOAD_BASE = "https://download.cms.gov/nppes/"

    # download.cms.gov refuses requests without a browser user agent.
    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"
        ),
    }

    # Only the monthly full-replacement file is ingested. Weekly incrementals
    # exist but each monthly file already supersedes them.
    MONTHLY_LINK_ID = "DDSMTH.ZIP.D"

    ARCHITECTURE_DIR = (
        REPO_ROOT / "models" / "us_hhs_nppes" / "code" / "architecture"
    )

    TABLES = [
        "provider",
        "taxonomy",
        "other_identifier",
        "other_name",
        "practice_location",
        "endpoint",
        "dicionario",
    ]

    # Tables that carry an extraction_date partition (dicionario does not).
    PARTITIONED_TABLES = [
        "provider",
        "taxonomy",
        "other_identifier",
        "other_name",
        "practice_location",
        "endpoint",
    ]

    # Rows per output parquet file. Keeps peak RAM bounded on write and keeps
    # the first staging blob small (see also the 00_header.parquet guard).
    CHUNK_ROWS = 500_000

    # CSV read block size (bytes) for the 11.6 GB main file.
    CSV_BLOCK_SIZE = 1 << 26
