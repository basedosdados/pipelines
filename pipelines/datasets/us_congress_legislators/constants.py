"""Constants for the us_congress_legislators recurring pipeline (Prefect 3).

Source: https://github.com/unitedstates/congress-legislators (CC0), a set of YAML
files republished in full on every commit.
"""

from enum import Enum


class constants(Enum):
    """Constants for the us_congress_legislators pipeline.

    Lowercase class name follows the repo-wide convention for dataset constant
    enums.
    """

    DATASET_ID = "us_congress_legislators"
    # The congress_member directory is built from the same files, so this flow
    # refreshes it too.
    DIRECTORY_DATASET_ID = "br_bd_diretorios_us"

    RAW_BASE_URL = "https://raw.githubusercontent.com/unitedstates/congress-legislators/main"
    COMMITS_API_URL = "https://api.github.com/repos/unitedstates/congress-legislators/commits"

    SOURCE_FILES = [
        "legislators-current",
        "legislators-historical",
        "legislators-social-media",
        "legislators-district-offices",
        "committees-current",
        "committees-historical",
        "committee-membership-current",
        "executive",
    ]

    # The table the source poll is anchored to (it carries the raw data source).
    POLL_TABLE_ID = "legislator"
