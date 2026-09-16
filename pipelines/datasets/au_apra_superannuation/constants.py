"""Constants for the au_apra_superannuation recurring pipeline.

The source is APRA's "Quarterly superannuation performance statistics" workbook,
a single .xlsx republished every quarter with a filename that embeds its coverage
window (e.g. "... - December 2004 to June 2026 _0.xlsx"). The filename therefore
changes each release, so the pipeline discovers the current link from the landing
page rather than hardcoding it (see utils.discover_source_url).
"""

from enum import Enum
from pathlib import Path

# repo-root-relative architecture dir (single source of truth for schema/order)
_ARCH = (
    Path(__file__).resolve().parents[3]
    / "models"
    / "au_apra_superannuation"
    / "code"
    / "architecture"
)


class constants(Enum):
    DATASET_ID = "au_apra_superannuation"

    LANDING_URL = "https://www.apra.gov.au/quarterly-superannuation-statistics"
    BASE_URL = "https://www.apra.gov.au"
    # match the performance workbook link, not MySuper / backseries files
    FILE_PATTERN = (
        r"Quarterly[%20 ]+superannuation[%20 ]+performance[%20 ]+statistics"
    )
    USER_AGENT = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
    )

    ARCHITECTURE_DIR = _ARCH

    # data tables (wide, one per financial statement) + the dictionary
    DATA_TABLES = [
        "financial_performance",
        "financial_position",
        "performance_ratios",
    ]
    TABLES = [
        "financial_performance",
        "financial_position",
        "performance_ratios",
        "dicionario",
    ]

    # fund type -> APRA table number; the sub-topic -> tab-suffix
    FUND_TABS = {
        "all": "1",
        "corporate": "2",
        "industry": "3",
        "public_sector": "4",
        "retail": "5",
    }
    STATEMENT_SUFFIX = {
        "financial_performance": "a",
        "financial_position": "b",
        "performance_ratios": "c",
    }

    FUND_LABELS = {
        "all": "All entities (whole superannuation industry)",
        "corporate": "Corporate funds",
        "industry": "Industry funds",
        "public_sector": "Public sector funds",
        "retail": "Retail funds",
    }
