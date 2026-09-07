"""Constants for the us_dol_oflc pipeline (Prefect 3).

U.S. Department of Labor, Office of Foreign Labor Certification case disclosure
data. See models/us_dol_oflc/CLAUDE.md for the design.
"""

from enum import Enum
from pathlib import Path

# Repo root, then the committed architecture and crosswalk CSVs — the schema
# source of truth for both this pipeline and the one-shot bootstrap.
_REPO_ROOT = Path(__file__).resolve().parents[3]
_CODE = _REPO_ROOT / "models" / "us_dol_oflc" / "code"


class constants(Enum):
    """Constants for the us_dol_oflc pipeline."""

    DATASET_ID = "us_dol_oflc"
    TABLES = ["lca", "perm", "h2a", "h2b", "dictionary"]
    PROGRAMS = ["lca", "perm", "h2a", "h2b"]

    ARCHITECTURE_DIR = _CODE / "architecture"
    CROSSWALK_DIR = _CODE / "crosswalk"

    # www.dol.gov sits behind Akamai, which rejects requests, curl and wget with
    # HTTP 403 on the TLS fingerprint alone — browser headers do not help. Every
    # fetch goes through curl_cffi impersonating Chrome.
    PERFORMANCE_PAGE = "https://www.dol.gov/agencies/eta/foreign-labor/performance"
    IMPERSONATE = "chrome"
    BASE_URL = "https://www.dol.gov"

    # Link text on the performance page is inconsistent, so files are found by
    # matching the href against these patterns, one per program.
    FILE_PATTERNS = {
        "lca": r"(LCA|H-1B|H1B|Icert)[_ ].*(Disclosure|Case_Data|iCert|FY)",
        "perm": r"PERM.*(Disclosure|FY)",
        "h2a": r"H-?2A.*(Disclosure|FY)",
        "h2b": r"H-?2B.*(Disclosure|FY)",
    }

    # A fiscal year is frozen once its final file has landed and the year is
    # over; only the open fiscal year is re-materialised on each run.
    FISCAL_YEAR_START_MONTH = 10
