"""Constants for the us_fed_fred recurring pipeline (Prefect 3).

FRED (Federal Reserve Bank of St. Louis) public-domain seed series. The curated
list and the two-filter license gate are documented in
``models/us_fed_fred/SEED_SERIES.md``; the full design is in
``models/us_fed_fred/ONBOARDING_PLAN.md``. The cleaning transform is shared with
the one-shot bootstrap in ``models/us_fed_fred/code/`` — see ``utils.py``.
"""

from enum import Enum
from pathlib import Path

# Repo root, then the committed architecture CSVs (the single schema source of
# truth — column order + bigquery_type per table).
_REPO_ROOT = Path(__file__).resolve().parents[3]


class constants(Enum):
    """Constants for the us_fed_fred pipeline.

    Lowercase class name follows the repo-wide convention for dataset constant
    enums. ``ARCHITECTURE_DIR`` points at the architecture CSVs under
    ``models/us_fed_fred/code/``, which are the schema source of truth for both
    this pipeline and the one-shot bootstrap.
    """

    DATASET_ID = "us_fed_fred"

    BASE_URL = "https://api.stlouisfed.org/fred"
    # ~100 req/min, safely under FRED's 120/min limit.
    MIN_INTERVAL = 0.6

    # FRED API key on the deployed worker: Vault secret path + key inside it.
    # Local/bootstrap runs read FRED_API_KEY from the environment instead.
    VAULT_SECRET_PATH = "fred"
    VAULT_SECRET_KEY = "FRED_API_KEY"

    # U.S. federal-agency sources whose works are public domain (17 U.S.C. §105).
    # Tuple (hashable) so it is a valid Enum value; used as a set at call sites.
    SOURCE_ALLOWLIST = (
        "Board of Governors of the Federal Reserve System (US)",
        "U.S. Bureau of Labor Statistics",
        "U.S. Bureau of Economic Analysis",
        "U.S. Census Bureau",
        "U.S. Department of the Treasury. Fiscal Service",
        "U.S. Office of Management and Budget",
        "U.S. Employment and Training Administration",
        "Federal Reserve Bank of St. Louis",
    )

    # The curated seed set — mirrors SEED_SERIES.md. Each is verified against the
    # license gate at download; any entry failing a filter is dropped and logged.
    SEED_SERIES = [
        # Board of Governors of the Federal Reserve System
        "FEDFUNDS",
        "DFF",
        "DGS10",
        "DGS2",
        "DGS3MO",
        "DTB3",
        "T10Y2Y",
        "T10Y3M",
        "WALCL",
        "M2SL",
        "M1SL",
        "BOGMBASE",
        "INDPRO",
        "TCU",
        "TOTALSL",
        "DEXUSEU",
        "DEXJPUS",
        "DEXCHUS",
        # U.S. Bureau of Labor Statistics
        "CPIAUCSL",
        "CPILFESL",
        "UNRATE",
        "U6RATE",
        "CIVPART",
        "EMRATIO",
        "PAYEMS",
        "MANEMP",
        "CES0500000003",
        "JTSJOL",
        "PPIACO",
        # U.S. Bureau of Economic Analysis
        "GDP",
        "GDPC1",
        "A191RL1Q225SBEA",
        "PCE",
        "PCEPI",
        "PCEPILFE",
        "DSPIC96",
        "PSAVERT",
        "CP",
        # U.S. Census Bureau
        "HOUST",
        "PERMIT",
        "RSAFS",
        "DGORDER",
        "TTLCONS",
        "BUSINV",
        # U.S. Treasury / Fiscal Service (GFDEGDQ188S, FYFSD resolve to OMB)
        "GFDEBTN",
        "GFDEGDQ188S",
        "MTSDS133FMS",
        "FYFSD",
        # U.S. Employment and Training Administration (DOL)
        "ICSA",
        # Federal Reserve Bank of St. Louis (derived, public)
        "USREC",
    ]

    # series catalog column order (architecture order). Kept here so the raw
    # download JSON and the parquet writer agree without re-reading the CSV.
    SERIES_COLS = [
        "series_id",
        "title",
        "units",
        "units_short",
        "frequency",
        "frequency_short",
        "seasonal_adjustment",
        "seasonal_adjustment_short",
        "observation_start",
        "observation_end",
        "last_updated",
        "source_name",
        "release_name",
        "notes",
    ]

    DATA_TABLES = ["observation", "series"]
    ALL_TABLES = ["observation", "series"]

    ARCHITECTURE_DIR = (
        _REPO_ROOT / "models" / "us_fed_fred" / "code" / "architecture"
    )
