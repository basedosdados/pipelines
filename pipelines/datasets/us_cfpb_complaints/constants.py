"""Constants for the us_cfpb_complaints recurring pipeline (Prefect 3).

CFPB Consumer Complaint Database. See models/us_cfpb_complaints/CLAUDE.md for the
full design, including why the documented REST API is unusable and why the CSV is
parsed with Python's ``csv`` rather than DuckDB.
"""

from enum import Enum
from pathlib import Path

# Repo root, then the committed architecture TSVs (the single schema source of
# truth — column order, bigquery_type and the raw -> clean name mapping).
_REPO_ROOT = Path(__file__).resolve().parents[3]


class constants(Enum):
    """Constants for the us_cfpb_complaints pipeline.

    Lowercase class name follows the repo-wide convention for dataset constant
    enums. ``ARCHITECTURE_DIR`` points at the architecture TSVs under
    ``models/us_cfpb_complaints/code/``, shared with the one-shot bootstrap.
    """

    DATASET_ID = "us_cfpb_complaints"

    COMPLAINT = "complaint"
    DICIONARIO = "dicionario"
    # Order matters: dbt builds complaint first, but every table is built before
    # any is tested — complaint's custom_dictionary_coverage test reads dicionario.
    ALL_TABLES = ["complaint", "dicionario"]

    ARCHITECTURE_DIR = _REPO_ROOT / "models" / "us_cfpb_complaints" / "code"

    # The full-database export, refreshed daily: ~1.4 GB zipped, 9.3 GB as CSV.
    # The documented REST API at www.consumerfinance.gov is behind Akamai and
    # returns "Access Denied" to every scripted client, so this is the only route.
    BULK_URL = "https://files.consumerfinance.gov/ccdb/complaints.csv.zip"
    ZIP_NAME = "complaints.csv.zip"
    CSV_NAME = "complaints.csv"

    # Columns enumerated in the `dicionario` table. The CFPB publishes these as
    # readable labels rather than codes, so `valor` repeats `chave`; the register
    # exists for `cobertura_temporal`, which exposes the April 2017 and August
    # 2023 revisions of the complaint form's taxonomy.
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

    # USPS abbreviation -> FIPS state code, from
    # `basedosdados.br_bd_diretorios_us.state` (`abbreviation`, `id_state`), the
    # directory `state_id` links to. The military post codes AA, AE and AP are
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

    # The CFPB publishes this one territory as a literal name where every other
    # row carries a two-letter code. Mapped to the same FIPS code as `UM`; the
    # published spelling stays untouched in `state_abbreviation`.
    STATE_ALIASES = {"UNITED STATES MINOR OUTLYING ISLANDS": "UM"}

    # Rows buffered per year before a parquet row group is flushed. Bounds peak
    # RAM at roughly (n_years x FLUSH_ROWS) rows held as Python strings.
    FLUSH_ROWS = 50_000
