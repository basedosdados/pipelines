"""Constants for the us_bea recurring pipeline (Prefect 3).

US Bureau of Economic Analysis (BEA) economic accounts, pulled directly from the
BEA REST API (https://apps.bea.gov/api/). Six tables: nipa, gdp_by_industry,
regional_state, regional_county, regional_metro, dicionario.

The API key is read from the ``BEA_API_KEY`` environment variable when present
(local development), and otherwise from HashiCorp Vault on the deployed worker —
see ``pipelines.datasets.us_bea.utils._key``.
"""

from enum import Enum
from pathlib import Path

# Repo root, then the committed architecture CSVs. These describe the FINAL
# (post-dbt) schema; the raw STAGING schema this pipeline writes differs
# (table_name/series_code, quarter/month as STRING) and lives in utils.py as
# ``STAGING_SCHEMAS`` — the dbt models rename/recast into the architecture shape.
_REPO_ROOT = Path(__file__).resolve().parents[3]


class constants(Enum):
    """Constants for the us_bea pipeline.

    Lowercase class name follows the repo-wide convention for dataset constant
    enums.
    """

    DATASET_ID = "us_bea"

    # BEA REST API.
    BASE_URL = "https://apps.bea.gov/api/data/"
    # Min seconds between call starts -> ~92/min, under BEA's 100/min cap.
    MIN_INTERVAL = 0.65

    # API key resolution. When BEA_API_KEY is absent from the environment (the
    # deployed worker), the key is read from Vault at VAULT_SECRET_PATH under
    # VAULT_KEY. The user must provision that secret before arming the schedule.
    ENV_KEY = "BEA_API_KEY"
    VAULT_SECRET_PATH = "us_bea"
    VAULT_KEY = "BEA_API_KEY"

    # Tokens the API uses for missing/suppressed values -> NULL.
    MISSING_TOKENS = ["", "(NA)", "(NM)", "(D)", "(L)", "(*)", "NA", "n/a"]

    # Data tables (partitioned parquet) + the static dictionary.
    DATA_TABLES = [
        "nipa",
        "gdp_by_industry",
        "regional_state",
        "regional_county",
        "regional_metro",
    ]
    ALL_TABLES = [
        "nipa",
        "gdp_by_industry",
        "regional_state",
        "regional_county",
        "regional_metro",
        "dicionario",
    ]

    # Regional families: the BEA "Regional" dataset holds many TableName codes;
    # each db-table pulls a prefix family at a fixed geographic wildcard.
    #   regional_state  = SA*/SQ*/PR*/TA* tables, GeoFips=STATE
    #   regional_county = CA* tables,             GeoFips=COUNTY
    #   regional_metro  = MA* tables,             GeoFips=MSA
    REGIONAL_FAMILIES = {
        "regional_state": {
            "prefixes": ["SA", "SQ", "PR", "TA"],
            "geofips": "STATE",
            "level": "state",
        },
        "regional_county": {
            "prefixes": ["CA"],
            "geofips": "COUNTY",
            "level": "county",
        },
        "regional_metro": {
            "prefixes": ["MA"],
            "geofips": "MSA",
            "level": "metro",
        },
    }

    # Rows buffered per table before a flush (bounds peak RAM; county is ~50M).
    FLUSH_ROWS = 500_000

    ARCHITECTURE_DIR = (
        _REPO_ROOT / "models" / "us_bea" / "code" / "architecture"
    )
