"""Constants for the us_census_bds pipeline."""

import os
from enum import Enum
from pathlib import Path


class constants(Enum):
    """Constants for the Census Business Dynamics Statistics pipeline."""

    DATASET_ID = "us_census_bds"

    # One directory per release; the release year is the last year covered.
    BASE_URL = (
        "https://www2.census.gov/programs-surveys/bds/tables/time-series/"
    )

    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
        )
    }

    # The server throttles each connection to ~65 KB/s but honours byte
    # ranges, so large files are fetched as many ranges in parallel.
    CHUNK_BYTES = 8 * 1024 * 1024
    WORKERS = 24

    DATA_DIR = Path(
        os.environ.get(
            "US_CENSUS_BDS_DATA_DIR",
            Path.home() / "Library" / "Caches" / "us_census_bds_data",
        )
    )

    ARCHITECTURE_DIR = (
        Path(__file__).resolve().parents[3]
        / "models"
        / "us_census_bds"
        / "code"
        / "architecture"
    )
