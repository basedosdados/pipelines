"""Constants for the us_cfpb_hmda recurring pipeline (Prefect 3).

HMDA Loan/Application Register (CFPB/FFIEC). Only the modern table
`loan_application_register` (2018+) refreshes: CFPB publishes one new year's
Snapshot National Loan-Level Dataset annually (~mid-year). The legacy table
(2007-2017) and the dicionario are frozen and are NOT touched by this pipeline.

The modern Snapshot files are immutable per year, so each run is a full replace
(dump_mode="overwrite") that re-cleans every modern year 2018..N into all-STRING
partitioned parquet. Overwrite keeps the staging schema consistently all-STRING
(the dbt model safe_casts) and mirrors the us_bls_cpi pattern; the per-year
streaming clean keeps peak disk/RAM low. See models/us_cfpb_hmda/CLAUDE.md.

Schema/column order/x1000 rules come from the architecture TSV - the single
source of truth shared with the one-shot bootstrap under models/us_cfpb_hmda/code.
"""

from enum import Enum
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[3]


class constants(Enum):
    """Constants for the us_cfpb_hmda pipeline (lowercase per repo convention)."""

    DATASET_ID = "us_cfpb_hmda"
    TABLE_ID = (
        "loan_application_register"  # the only table this pipeline refreshes
    )

    # data-browser nationwide CSV endpoint: 301 -> pre-generated per-year CSV on
    # files.ffiec.cfpb.gov. The `/nationwide/` variant needs no geo/LEI filter.
    MODERN_URL = "https://ffiec.cfpb.gov/v2/data-browser-api/view/nationwide/csv?years={year}"
    FIRST_YEAR = 2018  # modern (post-2017) schema starts here

    # Architecture TSV = schema source of truth (name, bigquery_type, original_name).
    ARCHITECTURE_TSV = (
        _REPO_ROOT
        / "models"
        / "us_cfpb_hmda"
        / "code"
        / "sheet_loan_application_register.tsv"
    )
    # Columns reported in thousands of dollars -> x1000 so measurement_unit=USD holds.
    MULTIPLY_1000 = ("income",)
