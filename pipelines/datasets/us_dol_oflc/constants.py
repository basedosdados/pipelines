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
    PERFORMANCE_PAGE = (
        "https://www.dol.gov/agencies/eta/foreign-labor/performance"
    )
    IMPERSONATE = "chrome"
    BASE_URL = "https://www.dol.gov"

    # Files are found by matching the published file name against these
    # patterns, one per program. They are anchored and deliberately narrow: the
    # performance page publishes companion workbooks beside each disclosure file
    # — LCA Appendix A and Worksites, the H-2A Addendums, the H-2B Appendixes —
    # which carry different layouts and, under a looser pattern, collapse onto
    # the same local name and overwrite the file we actually want.
    #
    # Only the case-level disclosure file is matched, and only in the modern
    # naming the pipeline ever sees: the historical layouts (H-1B_Case_Data_FY2008,
    # Icert_ LCA_ FY2009, LCA_FY2012_Q4 …) were onboarded once and are never
    # re-fetched, because a run only ever touches the open fiscal year and the
    # one before it.
    FILE_PATTERNS = {
        "lca": r"^LCA_Disclosure_Data_FY_?\d{4}(_Q[1-4])?\.xlsx?$",
        "perm": r"^PERM_Disclosure_Data_(New_Form_)?FY_?\d{4}(_Q[1-4])?\.xlsx?$",
        "h2a": r"^H-?2A_Disclosure_Data_FY_?\d{4}(_Q[1-4])?(_(new|old)_form)?\.xlsx?$",
        "h2b": r"^H-?2B_Disclosure_(Data_)?FY_?\d{4}(_Q[1-4])?\.xlsx?$",
    }

    # A fiscal year is frozen once its final file has landed and the year is
    # over; only the open fiscal year is re-materialised on each run.
    FISCAL_YEAR_START_MONTH = 10
