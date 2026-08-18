"""Constants for the us_sec_edgar pipeline."""

import os
from enum import Enum

_HERE = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.abspath(os.path.join(_HERE, "..", "..", ".."))


class constants(Enum):
    """Constants for us_sec_edgar."""

    DATASET_ID = "us_sec_edgar"

    # SEC fair-access rules require a descriptive User-Agent carrying a contact
    # address, and no more than 10 requests per second.
    # https://www.sec.gov/os/webmaster-faq#developers
    USER_AGENT = "Data Basis (Base dos Dados) rdahis@basedosdados.org"
    REQUEST_INTERVAL_SECONDS = 0.5

    INDEX_URL = (
        "https://www.sec.gov/data-research/sec-markets-data/"
        "financial-statement-data-sets"
    )
    ZIP_URL_TEMPLATE = (
        "https://www.sec.gov/files/dera/data/financial-statement-data-sets/"
        "{year}q{quarter}.zip"
    )

    # Source TSV file -> published table slug.
    SOURCE_FILES = {
        "sub.txt": "submission",
        "num.txt": "numeric_fact",
        "tag.txt": "tag",
        "pre.txt": "presentation",
    }
    TABLES = [
        "submission",
        "numeric_fact",
        "tag",
        "presentation",
        "dicionario",
    ]

    ARCHITECTURE_DIR = os.path.join(
        _REPO_ROOT, "models", "us_sec_edgar", "code", "architecture"
    )
    REFERENCE_DIR = os.path.join(
        _REPO_ROOT, "models", "us_sec_edgar", "code", "reference"
    )

    # 2009q1.zip exists but is header-only (the SEC ships it so every year has
    # four files); it produces no rows and is skipped.
    FIRST_QUARTER = (2009, 1)

    SCRATCH_DIR = os.environ.get(
        "US_SEC_EDGAR_DATA_DIR",
        os.path.expanduser("~/Downloads/us_sec_edgar_data"),
    )
