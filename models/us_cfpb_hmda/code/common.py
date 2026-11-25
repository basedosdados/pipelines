"""Shared paths and architecture-TSV parsing for the us_cfpb_hmda onboarding.

Scratch data lives under ~/Downloads/us_cfpb_hmda_data/ (never in the repo or Dropbox),
overridable via HMDA_DATA_DIR. The architecture TSVs in this directory are the source of
truth for column names, order, types, and the raw->clean name mapping.
"""

import csv
import os
from dataclasses import dataclass
from pathlib import Path

CODE_DIR = Path(__file__).resolve().parent
DATA_DIR = Path(
    os.environ.get(
        "HMDA_DATA_DIR", Path.home() / "Downloads" / "us_cfpb_hmda_data"
    )
)
INPUT = DATA_DIR / "input"
OUTPUT = DATA_DIR / "output"

MODERN = "loan_application_register"
LEGACY = "loan_application_register_legacy"

MODERN_YEARS = list(range(2018, 2025))  # 2018..2024
LEGACY_YEARS = list(range(2007, 2018))  # 2007..2017

SHEET = {
    MODERN: CODE_DIR / "sheet_loan_application_register.tsv",
    LEGACY: CODE_DIR / "sheet_loan_application_register_legacy.tsv",
}

# columns whose source is reported in thousands of dollars -> multiply by 1000 so the
# measurement_unit=USD is truthful. Keyed by (table, clean_name).
MULTIPLY_1000 = {
    (MODERN, "income"),
    (LEGACY, "loan_amount"),
    (LEGACY, "income"),
}


@dataclass
class Col:
    name: str  # clean BigQuery column name
    bq_type: str  # INT64 / FLOAT64 / STRING
    original: str  # raw header token in the source CSV


def load_cols(table: str) -> list[Col]:
    """Load ordered column specs from the architecture TSV for a table."""
    cols = []
    with open(SHEET[table], encoding="utf-8") as fh:
        for r in csv.DictReader(fh, delimiter="\t"):
            cols.append(
                Col(
                    r["name"].strip(),
                    r["bigquery_type"].strip().upper(),
                    r["original_name"].strip(),
                )
            )
    return cols
