"""Constants for the us_fhfa_hpi recurring pipeline (Prefect 3).

FHFA House Price Index® — the master file (monthly + quarterly, all published
levels) and the annual developmental indexes down to census tract.
See models/us_fhfa_hpi/ONBOARDING_PLAN.md for the full design.
"""

from enum import Enum
from pathlib import Path

# Repo root, then the committed architecture CSVs (the single schema source of
# truth — column order + bigquery_type per table).
_REPO_ROOT = Path(__file__).resolve().parents[3]


class constants(Enum):
    """Constants for the us_fhfa_hpi pipeline.

    Lowercase class name follows the repo-wide convention for dataset constant
    enums. ``ARCHITECTURE_DIR`` points at the architecture CSVs under
    ``models/us_fhfa_hpi/code/``, which are the schema source of truth for both
    this pipeline and the one-shot bootstrap.
    """

    DATASET_ID = "us_fhfa_hpi"

    # www.fhfa.gov serves the data files to plain clients, but a browser-like
    # User-Agent is used for consistency with the site's other endpoints.
    BASE_URL = "https://www.fhfa.gov"
    USER_AGENT = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120 Safari/537.36 rdahis@basedosdados.org"
    )

    # The master file appends every monthly and quarterly series FHFA publishes.
    MASTER_URL = "https://www.fhfa.gov/hpi/download/monthly/hpi_master.csv"

    # Annual developmental indexes: table slug -> (filename, sheet name).
    # ``sheet`` is None for the census tract file, which ships as CSV.
    ANNUAL_FILES = {
        "annual_national": ("hpi_at_national.xlsx", "national"),
        "annual_state": ("hpi_at_state.xlsx", "state"),
        "annual_cbsa": ("hpi_at_cbsa.xlsx", "cbsa"),
        "annual_county": ("hpi_at_county.xlsx", "county"),
        "annual_zip3": ("hpi_at_zip3.xlsx", "ZIP3"),
        "annual_zip5": ("hpi_at_zip5.xlsx", "ZIP5"),
        "annual_tract": ("hpi_at_tract.csv", None),
    }
    ANNUAL_BASE_URL = "https://www.fhfa.gov/hpi/download/annual"

    # Every annual workbook carries a five-row title preamble; the header is row 6.
    ANNUAL_HEADER_ROW = 5

    # Tables derived from the master file, and the filter that selects each.
    MASTER_TABLES = {
        "monthly_national": {"frequency": "monthly"},
        "quarterly_national": {
            "frequency": "quarterly",
            "level": ["USA or Census Division"],
        },
        "quarterly_state": {
            "frequency": "quarterly",
            "level": ["State", "Puerto Rico"],
        },
        "quarterly_metro": {"frequency": "quarterly", "level": ["MSA"]},
    }

    # Every table the pipeline builds, in dbt build order.
    TABLES = [
        "dicionario",
        "monthly_national",
        "quarterly_national",
        "quarterly_state",
        "quarterly_metro",
        "annual_national",
        "annual_state",
        "annual_cbsa",
        "annual_county",
        "annual_zip3",
        "annual_zip5",
        "annual_tract",
    ]

    ARCHITECTURE_DIR = (
        _REPO_ROOT / "models" / "us_fhfa_hpi" / "code" / "architecture"
    )
