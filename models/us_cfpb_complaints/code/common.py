"""Shared paths, architecture parsing and lookups for the us_cfpb_complaints onboarding.

Scratch data lives under ``~/Downloads/us_cfpb_complaints_data/`` (never in the repo
or in Dropbox), overridable via ``CFPB_COMPLAINTS_DATA_DIR``. The architecture TSVs
in this directory are the source of truth for column names, order, types and the
raw -> clean name mapping; they are produced by ``gen_architecture.py``.
"""

import csv
import os
from dataclasses import dataclass
from pathlib import Path

CODE_DIR = Path(__file__).resolve().parent
DATA_DIR = Path(
    os.environ.get(
        "CFPB_COMPLAINTS_DATA_DIR",
        Path.home() / "Downloads" / "us_cfpb_complaints_data",
    )
)
INPUT = DATA_DIR / "input"
OUTPUT = DATA_DIR / "output"

DATASET_ID = "us_cfpb_complaints"
COMPLAINT = "complaint"
DICIONARIO = "dicionario"

CSV_NAME = "complaints.csv"
ZIP_NAME = "complaints.csv.zip"
BULK_URL = "https://files.consumerfinance.gov/ccdb/complaints.csv.zip"

# Columns whose values are enumerated in the `dicionario` table. The CFPB publishes
# these as readable labels rather than codes, so `dicionario.valor` repeats
# `dicionario.chave`; the information the dictionary adds is `cobertura_temporal`,
# which records the taxonomy revisions of April 2017 and August 2023.
DICT_COLUMNS = [
    "product",
    "sub_product",
    "issue",
    "sub_issue",
    "tags",
    "submitted_via",
    "company_public_response",
    "company_response_to_consumer",
    "timely_response",
]

# USPS abbreviation -> FIPS state code, taken from
# `basedosdados.br_bd_diretorios_us.state` (columns `abbreviation`, `id_state`),
# which is the directory `state_id` links to. Military post codes (AA, AE, AP) are
# deliberately absent: they are not states and have no FIPS code.
STATE_FIPS = {
    "AK": "02",
    "AL": "01",
    "AR": "05",
    "AS": "60",
    "AZ": "04",
    "CA": "06",
    "CO": "08",
    "CT": "09",
    "DC": "11",
    "DE": "10",
    "FL": "12",
    "FM": "64",
    "GA": "13",
    "GU": "66",
    "HI": "15",
    "IA": "19",
    "ID": "16",
    "IL": "17",
    "IN": "18",
    "KS": "20",
    "KY": "21",
    "LA": "22",
    "MA": "25",
    "MD": "24",
    "ME": "23",
    "MH": "68",
    "MI": "26",
    "MN": "27",
    "MO": "29",
    "MP": "69",
    "MS": "28",
    "MT": "30",
    "NC": "37",
    "ND": "38",
    "NE": "31",
    "NH": "33",
    "NJ": "34",
    "NM": "35",
    "NV": "32",
    "NY": "36",
    "OH": "39",
    "OK": "40",
    "OR": "41",
    "PA": "42",
    "PR": "72",
    "PW": "70",
    "RI": "44",
    "SC": "45",
    "SD": "46",
    "TN": "47",
    "TX": "48",
    "UM": "74",
    "UT": "49",
    "VA": "51",
    "VI": "78",
    "VT": "50",
    "WA": "53",
    "WI": "55",
    "WV": "54",
    "WY": "56",
}

# The CFPB publishes this one territory as a literal name where every other row
# carries a two-letter code. Mapped to the same FIPS code as `UM`; the published
# spelling is preserved untouched in `state_abbreviation`.
STATE_ALIASES = {"UNITED STATES MINOR OUTLYING ISLANDS": "UM"}


@dataclass
class Col:
    """One column of an architecture table."""

    name: str  # clean BigQuery column name
    bq_type: str  # INT64 / STRING / DATE
    original: str  # header token in the source CSV, empty if derived
    covered_by_dictionary: bool
    directory_column: str
    measurement_unit: str


def load_cols(table: str) -> list[Col]:
    """Load ordered column specs from the architecture TSV for a table."""
    path = CODE_DIR / f"sheet_{table}.tsv"
    cols = []
    with open(path, encoding="utf-8") as fh:
        for r in csv.DictReader(fh, delimiter="\t"):
            cols.append(
                Col(
                    name=r["name"].strip(),
                    bq_type=r["bigquery_type"].strip().upper(),
                    original=r["original_name"].strip(),
                    covered_by_dictionary=r["covered_by_dictionary"].strip()
                    == "yes",
                    directory_column=r["directory_column"].strip(),
                    measurement_unit=r["measurement_unit"].strip(),
                )
            )
    return cols


def state_id_for(published_state: str) -> str | None:
    """Map a published `State` value to its FIPS code, or None when there is none.

    Returns None for the empty value, for the military post codes AA/AE/AP, and for
    anything else absent from the directory — never a guess.
    """
    v = (published_state or "").strip()
    if not v:
        return None
    v = STATE_ALIASES.get(v.upper(), v)
    return STATE_FIPS.get(v.upper())
