"""Constants for the us_eia_seds recurring pipeline (Prefect 3).

U.S. Energy Information Administration **State Energy Data System (SEDS)** — the
complete state-by-state energy consumption, price and expenditure series across
all fuels, 1960 to the latest complete year. See ``models/us_eia_seds/CLAUDE.md``
for the full design.

One long source file (``Complete_SEDS.csv``: ``Data_Status, MSN, StateCode, Year,
Data``) plus a codes workbook decoding the 5-character MSN. SEDS restates the
whole series on every release, so the pipeline rebuilds every year partition from
the newest file rather than appending.
"""

from enum import Enum
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[3]
_CODE_DIR = _REPO_ROOT / "models" / "us_eia_seds" / "code"


class constants(Enum):
    """Constants for the us_eia_seds pipeline.

    Lowercase class name follows the repo-wide convention for dataset constant
    enums.
    """

    DATASET_ID = "us_eia_seds"

    SEDS_CONSUMPTION = "seds_consumption"
    DICIONARIO = "dicionario"
    # Order matters only in that every table is built before any is tested: the
    # data table's custom_dictionary_coverage test reads dicionario.
    ALL_TABLES = ["seds_consumption", "dicionario"]
    DATA_TABLES = ["seds_consumption"]

    # Committed architecture CSVs — the schema source of truth (column order,
    # bigquery_type, descriptions). gen_architecture.py writes them.
    ARCHITECTURE_DIR = _CODE_DIR / "architecture"
    # Vendored decode tables, dumped once from Codes_and_Descriptions.xlsx by
    # vendor_seds_codes (see models/us_eia_seds/code/seds_codes/).
    SEDS_CODES_DIR = _CODE_DIR / "seds_codes"
    MSN_CODES = SEDS_CODES_DIR / "msn_codes.csv"
    STATE_CODES = SEDS_CODES_DIR / "state_codes.csv"
    # (state abbreviation, id_state) export of br_bd_diretorios_us, reused from
    # us_eia_electricity. SEDS publishes a 2-letter postal code, resolved to FIPS
    # by that abbreviation.
    COUNTY_DIRECTORY = _CODE_DIR / "us_county_directory.csv"

    FIRST_YEAR = 1960

    # The single complete data file and the codes workbook, both under the SEDS
    # "CDF" (complete data files) directory.
    SEDS_BASE = "https://www.eia.gov/state/seds/CDF"
    COMPLETE_CSV_URL = f"{SEDS_BASE}/Complete_SEDS.csv"
    CODES_URL = f"{SEDS_BASE}/Codes_and_Descriptions.xlsx"
    # The landing page carries the release/next-release dates polled to decide
    # whether a new vintage has landed.
    SEDS_PAGE_URL = "https://www.eia.gov/state/seds/seds-data-complete.php"

    # eia.gov serves the CSV to a plain client, but the HTML index pages sit
    # behind a CDN that has 403'd default library agents.
    USER_AGENT = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    )

    # MSN character 5 -> measure_type slug. The fifth character encodes what the
    # series measures and therefore the family its unit belongs to. Measured over
    # all 968 MSNs in the codes workbook; every fifth character present maps here,
    # and the transform raises on any it does not recognise so a new EIA measure
    # code cannot slip through as a silent null.
    MEASURE_TYPE = {
        "B": "consumption_btu",
        "P": "consumption_physical",
        "D": "price",
        "V": "expenditure",
        "E": "co2_emissions",
        "K": "electricity",
        "S": "capacity",
        "R": "conversion_factor",
        "N": "number",
        "X": "other",
    }
