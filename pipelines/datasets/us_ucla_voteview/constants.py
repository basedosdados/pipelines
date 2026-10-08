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

    ARCHITECTURE_DIR = _CODE_DIR / "architecture"
    DICTIONARY_CSV = _CODE_DIR / "dictionary.csv"

    # Scratch data never goes under Dropbox (see onboarding-workflow).
    DATA_DIR = Path(
        os.environ.get(
            "US_UCLA_VOTEVIEW_DATA_DIR",
            Path.home() / "Library" / "Caches" / "us_ucla_voteview_data",
        )
    )
