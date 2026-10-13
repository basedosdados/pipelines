"""Constants for the us_epu_gpr recurring pipeline (Prefect 3).

Two newspaper-based uncertainty families on one monthly (and daily) axis:

- **EPU** — Economic Policy Uncertainty (Baker, Bloom and Davis), global,
  country and category-specific indices, hosted at policyuncertainty.com.
- **GPR** — Geopolitical Risk (Caldara and Iacoviello, Federal Reserve Board),
  global (recent from 1985, historical from 1900), country and daily, hosted at
  matteoiacoviello.com.

See models/us_epu_gpr/code/ONBOARDING_PLAN.md for the full design.
"""

from enum import Enum
from pathlib import Path

# Repo root, then the committed architecture CSVs (the single schema source of
# truth — column order + bigquery_type per table).
_REPO_ROOT = Path(__file__).resolve().parents[3]


class constants(Enum):
    """Constants for the us_epu_gpr pipeline.

    Lowercase class name follows the repo-wide convention for dataset constant
    enums. ``ARCHITECTURE_DIR`` points at the architecture CSVs under
    ``models/us_epu_gpr/code/``, the schema source of truth for both this
    pipeline and the one-shot bootstrap.
    """

    DATASET_ID = "us_epu_gpr"

    # policyuncertainty.com sits behind a GoDaddy/Sucuri WAF that serves an
    # interstitial for HTML page paths but allows direct /media/ file downloads.
    # A browser User-Agent avoids edge-case blocks. matteoiacoviello.com is
    # unprotected.
    USER_AGENT = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120 Safari/537.36 rdahis@basedosdados.org"
    )

    EPU_BASE = "https://www.policyuncertainty.com/media"
    GPR_BASE = "https://www.matteoiacoviello.com/gpr_files"

    # filename -> source URL. Downloaded into input/.
    SOURCE_FILES = {
        "Global_Policy_Uncertainty_Data.xlsx": f"{EPU_BASE}/Global_Policy_Uncertainty_Data.xlsx",
        "US_Policy_Uncertainty_Data.xlsx": f"{EPU_BASE}/US_Policy_Uncertainty_Data.xlsx",
        "All_Country_Data.xlsx": f"{EPU_BASE}/All_Country_Data.xlsx",
        "Categorical_EPU_Data.xlsx": f"{EPU_BASE}/Categorical_EPU_Data.xlsx",
        "All_Daily_Policy_Data.csv": f"{EPU_BASE}/All_Daily_Policy_Data.csv",
        "data_gpr_export.xls": f"{GPR_BASE}/data_gpr_export.xls",
        "data_gpr_daily_recent.xls": f"{GPR_BASE}/data_gpr_daily_recent.xls",
    }

    DATA_TABLES = ["index_monthly", "index_daily"]
    ALL_TABLES = ["index_monthly", "index_daily", "dicionario"]

    ARCHITECTURE_DIR = (
        _REPO_ROOT / "models" / "us_epu_gpr" / "code" / "architecture"
    )
