"""Constants for the us_epa_ghgrp recurring pipeline (Prefect 3).

EPA Greenhouse Gas Reporting Program (GHGRP) — annual facility-level greenhouse
gas emissions, read from the Envirofacts GHG REST API. See
models/us_epa_ghgrp/ONBOARDING_PLAN.md for the full design.
"""

from enum import Enum
from pathlib import Path

# Repo root, then the committed architecture CSVs (the single schema source of
# truth — column order + bigquery_type per table).
_REPO_ROOT = Path(__file__).resolve().parents[3]


class constants(Enum):
    """Constants for the us_epa_ghgrp pipeline.

    Lowercase class name follows the repo-wide convention for dataset constant
    enums. ``ARCHITECTURE_DIR`` points at the architecture CSVs under
    ``models/us_epa_ghgrp/code/``, which are the schema source of truth for both
    this pipeline and the one-shot bootstrap.
    """

    DATASET_ID = "us_epa_ghgrp"

    # Envirofacts REST API. The GHG "pub_*" tables are the published FLIGHT data
    # model: one facility dimension, one row per facility-year, and two fact
    # tables of CO2e emissions — by subpart and by sector/subsector.
    # https://www.epa.gov/enviro/greenhouse-gas-model
    API_BASE = "https://data.epa.gov/efservice"
    USER_AGENT = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120 Safari/537.36 rdahis@basedosdados.org"
    )

    # Small lookup tables, fetched whole.
    DIM_TABLES = [
        "pub_dim_sector",
        "pub_dim_subsector",
        "pub_dim_ghg",
        "pub_dim_subpart",
    ]
    # Large tables, fetched one reporting year at a time in row chunks. The API
    # returns whole result sets for these sizes, but the chunking keeps a single
    # request well under any server-side cap.
    FACT_TABLES = [
        "pub_dim_facility",
        "pub_facts_subp_ghg_emission",
        "pub_facts_sector_ghg_emission",
    ]
    ROW_CHUNK = 50_000

    # First reporting year of the program.
    FIRST_YEAR = 2010

    # Every table the pipeline builds, in dbt build order.
    TABLES = ["dicionario", "facility", "emission_subpart", "emission_sector"]

    ARCHITECTURE_DIR = (
        _REPO_ROOT / "models" / "us_epa_ghgrp" / "code" / "architecture"
    )

    # USPS state abbreviation -> two-digit FIPS code, matching
    # br_bd_diretorios_us.state. The API carries only the abbreviation; the
    # county FIPS (when present) is the same code prefixed.
    STATE_FIPS = {
        "AL": "01", "AK": "02", "AZ": "04", "AR": "05", "CA": "06", "CO": "08",
        "CT": "09", "DE": "10", "DC": "11", "FL": "12", "GA": "13", "HI": "15",
        "ID": "16", "IL": "17", "IN": "18", "IA": "19", "KS": "20", "KY": "21",
        "LA": "22", "ME": "23", "MD": "24", "MA": "25", "MI": "26", "MN": "27",
        "MS": "28", "MO": "29", "MT": "30", "NE": "31", "NV": "32", "NH": "33",
        "NJ": "34", "NM": "35", "NY": "36", "NC": "37", "ND": "38", "OH": "39",
        "OK": "40", "OR": "41", "PA": "42", "RI": "44", "SC": "45", "SD": "46",
        "TN": "47", "TX": "48", "UT": "49", "VT": "50", "VA": "51", "WA": "53",
        "WV": "54", "WI": "55", "WY": "56", "AS": "60", "FM": "64", "GU": "66",
        "MH": "68", "MP": "69", "PW": "70", "PR": "72", "UM": "74", "VI": "78",
    }  # fmt: skip
