"""Constants for the us_treasury_usaspending recurring pipeline (Prefect 3).

USAspending.gov Award Data Archive — every federal contract and financial
assistance transaction, one zip per fiscal year per award family.
See models/us_treasury_usaspending/code/ for the one-shot bootstrap that shares
this module's transform.
"""

from enum import Enum
from pathlib import Path

# Repo root, then the committed architecture CSVs (the single schema source of
# truth — column order + bigquery_type per table).
_REPO_ROOT = Path(__file__).resolve().parents[3]


class constants(Enum):
    """Constants for the us_treasury_usaspending pipeline.

    Lowercase class name follows the repo-wide convention for dataset constant
    enums.
    """

    DATASET_ID = "us_treasury_usaspending"

    # Public S3 bucket behind the "Award Data Archive" download page. The
    # monthly-files API tells us the current stamp; the bucket serves the files.
    ARCHIVE_BASE = "https://files.usaspending.gov/award_data_archive"
    MONTHLY_FILES_API = (
        "https://api.usaspending.gov/api/v2/bulk_download/list_monthly_files/"
    )

    # award family in the archive file name -> BD table slug
    AWARD_FAMILIES = {
        "Contracts": "contract_transaction",
        "Assistance": "assistance_transaction",
    }

    TABLES = ["contract_transaction", "assistance_transaction", "dicionario"]

    # Fiscal years published by the archive. FY2007 is the first.
    FIRST_FISCAL_YEAR = 2007

    ARCHITECTURE_DIR = (
        _REPO_ROOT
        / "models"
        / "us_treasury_usaspending"
        / "code"
        / "architecture"
    )

    PARTITION_COLUMN = "fiscal_year"

    # Source column renamed onto the partition column.
    SOURCE_PARTITION_COLUMN = "action_date_fiscal_year"

    # Column used for temporal coverage and the BD Pro rolling window.
    COVERAGE_DATE_COLUMN = "action_date"

    # Rows per parquet row group. The transaction tables are wide (297 columns
    # for contracts), so a smaller group keeps writer memory bounded.
    ROW_GROUP_SIZE = 200_000

    # Rows per CSV read batch.
    READ_BATCH_SIZE = 100_000
