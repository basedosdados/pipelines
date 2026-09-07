"""Constants for the us_state_foreign_assistance dataset (ForeignAssistance.gov)."""

from enum import Enum
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[3]


class constants(Enum):
    """Constant values for the us_state_foreign_assistance pipeline.

    ForeignAssistance.gov publishes its bulk files on a public S3 bucket
    (``files.explorer.devtechlab.com``); the website itself is a JavaScript
    app with no download endpoint of its own. Each release replaces the
    whole history, so the ``Last-Modified`` header of the complete file is
    the freshness signal.
    """

    DATASET_ID = "us_state_foreign_assistance"
    SOURCE_URL = "https://foreignassistance.gov/data"
    S3_BASE = "https://s3.amazonaws.com/files.explorer.devtechlab.com"
    FILES = {
        "transaction": "us_foreign_aid_complete.csv",
        "budget": "us_foreign_budget_complete.csv",
    }
    DATA_DICTIONARY = "DataDictionary_ForeignAssistancegov.pdf"
    TABLES = ["transaction", "budget", "dicionario"]
    ARCHITECTURE_DIR = (
        _REPO_ROOT
        / "models"
        / "us_state_foreign_assistance"
        / "code"
        / "architecture"
    )
    DEFAULT_DATA_DIR = (
        Path.home() / "Downloads" / "us_state_foreign_assistance_data"
    )
    # Former states whose 3-letter code is not an ISO 3166-1 alpha-3 code in
    # the world country directory (Serbia and Montenegro, Yugoslavia, Sudan
    # before the 2011 split). Regions carry 4-digit country ids and are
    # excluded by that rule instead.
    NON_ISO_CODES = ("SCG", "YUF", "SDF")
