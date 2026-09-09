"""Constants for the us_census_bps pipeline."""

from enum import Enum
from pathlib import Path


class constants(Enum):
    """Constants for the Census Building Permits Survey pipeline."""

    DATASET_ID = "us_census_bps"

    BASE_URL = "https://www2.census.gov/econ/bps/"

    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
        )
    }

    # Region directories and their file-name prefixes for the place files.
    PLACE_REGIONS = {
        "Northeast Region": "ne",
        "Midwest Region": "mw",
        "South Region": "so",
        "West Region": "we",
    }

    # Directory on the Census server for each non-place geography level.
    GEO_DIRS = {
        "state": "State",
        "county": "County",
        "metro": "Metro (ending 2023)",
        "cbsa": "CBSA (beginning Jan 2024)",
    }

    # File-name prefix for each non-place geography level.
    GEO_PREFIXES = {
        "state": "st",
        "county": "co",
        "metro": "ma",
        "cbsa": "cbsa",
    }

    # First survey year available per level and periodicity.
    FIRST_YEAR = {
        ("state", "monthly"): 1988,
        ("state", "annual"): 1980,
        ("county", "monthly"): 2000,
        ("county", "annual"): 1990,
        ("metro", "monthly"): 1988,
        ("metro", "annual"): 1980,
        ("cbsa", "monthly"): 2024,
        ("cbsa", "annual"): 2024,
        ("place", "monthly"): 1988,
        ("place", "annual"): 1980,
    }

    # Valuation is published in thousands of dollars at these levels.
    THOUSANDS_LEVELS = ("state", "metro", "cbsa")

    # Census structure type codes, in published column order.
    STRUCTURE_TYPES = ("101", "103", "104", "105")

    ARCHITECTURE_DIR = (
        Path(__file__).resolve().parents[3]
        / "models"
        / "us_census_bps"
        / "code"
        / "architecture"
    )

    TABLES = (
        "permit_place_monthly",
        "permit_place_annual",
        "permit_county_monthly",
        "permit_county_annual",
        "permit_cbsa_monthly",
        "permit_cbsa_annual",
        "permit_msa_monthly",
        "permit_msa_annual",
        "permit_state_monthly",
        "permit_state_annual",
        "dicionario",
    )
