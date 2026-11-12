"""Constants for us_ssa_baby_names (SSA baby names, Social Security Administration).

Shared by the one-shot onboarding under ``models/us_ssa_baby_names/code/`` and a
future recurring Prefect pipeline in this package.
"""

import os
from enum import Enum
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[3]
_CODE_DIR = _REPO_ROOT / "models" / "us_ssa_baby_names" / "code"


class constants(Enum):
    """Constants for the us_ssa_baby_names dataset.

    ``ARCHITECTURE_DIR`` holds the architecture CSVs, the schema source of truth
    (column order and ``bigquery_type``) for the onboarding and the pipeline.
    """

    DATASET_ID = "us_ssa_baby_names"

    # SSA republishes all three archives in full every May, with the new year
    # appended. www.ssa.gov sits behind Akamai, which 403s plain HTTP clients.
    BASE_URL = "https://www.ssa.gov/oact/babynames/"
    NATIONAL_ZIP = "names.zip"
    STATE_ZIP = "namesbystate.zip"
    TERRITORY_ZIP = "namesbyterritory.zip"
    # archive -> path under BASE_URL (the state and territory files sit in
    # subdirectories; the bare file names 404)
    SOURCE_PATHS = {
        NATIONAL_ZIP: "names.zip",
        STATE_ZIP: "state/namesbystate.zip",
        TERRITORY_ZIP: "territory/namesbyterritory.zip",
    }

    TABLES = ["first_name", "first_name_state", "dictionary"]
    PARTITION_COL = "year"

    # Postal abbreviation -> FIPS code, as in br_bd_diretorios_us.state
    # (abbreviation -> id_state). The state archive has the 50 states and DC;
    # the territory archive has PR and TR, SSA's aggregate of AS, GU, MP and VI,
    # which has no FIPS code (state_id NULL).
    TERRITORY_AGGREGATE = "TR"
    STATE_FIPS = {
        "AK": "02", "AL": "01", "AR": "05", "AS": "60", "AZ": "04", "CA": "06",
        "CO": "08", "CT": "09", "DC": "11", "DE": "10", "FL": "12", "GA": "13",
        "GU": "66", "HI": "15", "IA": "19", "ID": "16", "IL": "17", "IN": "18",
        "KS": "20", "KY": "21", "LA": "22", "MA": "25", "MD": "24", "ME": "23",
        "MI": "26", "MN": "27", "MO": "29", "MP": "69", "MS": "28", "MT": "30",
        "NC": "37", "ND": "38", "NE": "31", "NH": "33", "NJ": "34", "NM": "35",
        "NV": "32", "NY": "36", "OH": "39", "OK": "40", "OR": "41", "PA": "42",
        "PR": "72", "RI": "44", "SC": "45", "SD": "46", "TN": "47", "TX": "48",
        "UT": "49", "VA": "51", "VI": "78", "VT": "50", "WA": "53", "WI": "55",
        "WV": "54", "WY": "56",
    }  # fmt: skip

    ARCHITECTURE_DIR = _CODE_DIR / "architecture"

    # Scratch data never goes under Dropbox (see onboarding-workflow).
    DATA_DIR = Path(
        os.environ.get(
            "US_SSA_BABY_NAMES_DATA_DIR",
            Path.home() / "Library" / "Caches" / "us_ssa_baby_names_data",
        )
    )
