"""Constants for us_ucla_voteview (Voteview congressional roll-call votes, UCLA).

Shared by the one-shot onboarding under ``models/us_ucla_voteview/code/`` and the
recurring Prefect pipeline in this package.
"""

import os
from enum import Enum
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[3]
_CODE_DIR = _REPO_ROOT / "models" / "us_ucla_voteview" / "code"


class constants(Enum):
    """Constants for the us_ucla_voteview dataset.

    ``ARCHITECTURE_DIR`` holds the architecture CSVs, the schema source of truth
    (column order and ``bigquery_type``) for both the onboarding and the pipeline.
    """

    DATASET_ID = "us_ucla_voteview"

    # Voteview rebuilds these "all congresses, all chambers" files from scratch on
    # every release, so each download is the full history.
    BASE_URL = "https://voteview.com/static/data/out"
    SOURCE_TABLES = ["members", "parties", "rollcalls", "votes"]

    # Partitioned by congress (hive directories congress=<n>/).
    PARTITIONED_TABLES = ["rollcalls", "votes"]
    ALL_TABLES = ["members", "parties", "rollcalls", "votes", "dictionary"]
    PARTITION_COL = "congress"

    # Integer-valued columns that the source sometimes prints as "100.0". They are
    # normalised to "100" before writing, whatever their BigQuery type.
    INTEGER_LIKE = {
        "congress",
        "icpsr",
        "state_icpsr",
        "district_code",
        "party_code",
        "occupancy",
        "last_means",
        "conditional",
        "born",
        "died",
        "nominate_number_of_votes",
        "nominate_number_of_errors",
        "rollnumber",
        "session",
        "clerk_rollnumber",
        "yea_count",
        "nay_count",
        "cast_code",
        "n_members",
    }

    # State postal abbreviation -> FIPS code, as in br_bd_diretorios_us.state
    # (abbreviation -> id_state). Fills members.state_id, the directory link.
    # Presidents carry "USA", which maps to nothing.
    STATE_FIPS = {
        "AK": "02", "AL": "01", "AR": "05", "AS": "60", "AZ": "04", "CA": "06",
        "CO": "08", "CT": "09", "DC": "11", "DE": "10", "FL": "12", "FM": "64",
        "GA": "13", "GU": "66", "HI": "15", "IA": "19", "ID": "16", "IL": "17",
        "IN": "18", "KS": "20", "KY": "21", "LA": "22", "MA": "25", "MD": "24",
        "ME": "23", "MH": "68", "MI": "26", "MN": "27", "MO": "29", "MP": "69",
        "MS": "28", "MT": "30", "NC": "37", "ND": "38", "NE": "31", "NH": "33",
        "NJ": "34", "NM": "35", "NV": "32", "NY": "36", "OH": "39", "OK": "40",
        "OR": "41", "PA": "42", "PR": "72", "PW": "70", "RI": "44", "SC": "45",
        "SD": "46", "TN": "47", "TX": "48", "UM": "74", "UT": "49", "VA": "51",
        "VI": "78", "VT": "50", "WA": "53", "WI": "55", "WV": "54", "WY": "56",
    }  # fmt: skip

    ARCHITECTURE_DIR = _CODE_DIR / "architecture"
    DICTIONARY_CSV = _CODE_DIR / "dictionary.csv"

    # Scratch data never goes under Dropbox (see onboarding-workflow).
    DATA_DIR = Path(
        os.environ.get(
            "US_UCLA_VOTEVIEW_DATA_DIR",
            Path.home() / "Library" / "Caches" / "us_ucla_voteview_data",
        )
    )
