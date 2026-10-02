"""Constants for us_bls_cex (US BLS Consumer Expenditure Surveys).

Only the LABSTAT layer (``series``, ``annual``) is refreshed by a recurring
pipeline; the PUMD microdata and ``ucc`` tables are one-shot loads from
``models/us_bls_cex/code/``.
"""

from enum import Enum
from pathlib import Path

# Repo root, then the committed architecture CSVs (schema source of truth).
_REPO_ROOT = Path(__file__).resolve().parents[3]


class constants(Enum):
    """Constants for the us_bls_cex LABSTAT pipeline."""

    DATASET_ID = "us_bls_cex"

    # download.bls.gov 403s without a browser User-Agent; BLS asks for a contact
    # email in the UA string.
    BASE_URL = "https://download.bls.gov/pub/time.series/cx"
    USER_AGENT = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120 Safari/537.36 rdahis@basedosdados.org"
    )

    # LABSTAT flat files used by the transform. cx.data.1.AllData holds every
    # published mean; cx.aspect holds standard errors, shares, aggregates and
    # percent reporting for the same (series, year).
    LABSTAT_FILES = [
        "cx.series",
        "cx.category",
        "cx.subcategory",
        "cx.item",
        "cx.demographics",
        "cx.characteristics",
        "cx.process",
        "cx.footnote",
        "cx.data.1.AllData",
        "cx.aspect",
    ]

    # cx.aspect aspect_type -> annual column
    ASPECT_COLUMNS = {
        "E": "standard_error",
        "R0": "relative_standard_error",
        "ES": "expenditure_share",
        "AG": "aggregate_expenditure",
        "AS": "aggregate_share",
        "RP": "percent_reporting",
    }

    LABSTAT_TABLES = ["series", "annual"]

    ARCHITECTURE_DIR = (
        _REPO_ROOT / "models" / "us_bls_cex" / "code" / "architecture"
    )
