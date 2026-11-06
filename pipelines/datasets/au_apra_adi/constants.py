"""Constants for the au_apra_adi recurring pipeline.

Source: APRA's "Quarterly authorised deposit-taking institution performance"
workbook, one .xlsx republished each quarter with a coverage-window filename
(e.g. "... -September 2004 to March 2026.xlsx"), so the pipeline discovers the
current link from the landing page rather than hardcoding it.

Structure: each data tab is one (institution type x sub-topic). The institution
type comes from the tab's table number (titles overlap — "Credit unions" appears
for both table 3 and the discontinued A.2 — so the number, not the title text, is
authoritative). The sub-topic comes from the tab title keywords.
"""

from enum import Enum
from pathlib import Path

_ARCH = (
    Path(__file__).resolve().parents[3]
    / "models"
    / "au_apra_adi"
    / "code"
    / "architecture"
)


class constants(Enum):
    DATASET_ID = "au_apra_adi"

    LANDING_URL = "https://www.apra.gov.au/quarterly-authorised-deposit-taking-institution-statistics"
    BASE_URL = "https://www.apra.gov.au"
    FILE_PATTERN = r"[Qq]uarterly[%20 ]+authorised[%20 ]+deposit-taking[%20 ]+institution[%20 ]+performance"
    USER_AGENT = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
    )

    ARCHITECTURE_DIR = _ARCH

    # table number (from the tab id) -> institution type code
    TAB_TYPE = {
        "1": "all_adis",
        "2": "banks",
        "3": "credit_unions_building_societies",
        "4": "major_banks",
        "5": "other_domestic_banks",
        "6": "foreign_subsidiary_banks",
        "7": "foreign_branch_banks",
        "8": "mutual_adis",
        "A.1": "building_societies",
        "A.2": "credit_unions",
    }

    INSTITUTION_LABELS = {
        "all_adis": "All ADIs",
        "banks": "Banks",
        "credit_unions_building_societies": "Credit unions and building societies",
        "major_banks": "Major banks",
        "other_domestic_banks": "Other domestic banks",
        "foreign_subsidiary_banks": "Foreign subsidiary banks",
        "foreign_branch_banks": "Foreign branch banks",
        "mutual_adis": "Mutual ADIs",
        "building_societies": "Building societies (discontinued series)",
        "credit_unions": "Credit unions (discontinued series)",
    }

    WIDE_TABLES = [
        "financial_performance",
        "financial_position",
        "performance_ratios",
    ]
    LONG_TABLES = [
        "capital_adequacy",
        "asset_quality",
        "liquidity_lcr",
        "liquidity_mlh",
    ]
    DATA_TABLES = [
        "financial_performance",
        "financial_position",
        "capital_adequacy",
        "asset_quality",
        "liquidity_lcr",
        "liquidity_mlh",
        "performance_ratios",
    ]
    TABLES = [*DATA_TABLES, "dicionario"]

    SUBTOPIC_LABELS = {
        "financial_performance": "Financial performance",
        "financial_position": "Financial position",
        "capital_adequacy": "Capital adequacy",
        "asset_quality": "Asset quality",
        "liquidity_lcr": "Liquidity — Liquidity Coverage Ratio (LCR)",
        "liquidity_mlh": "Liquidity — Minimum Liquidity Holdings (MLH)",
        "performance_ratios": "Performance ratios",
    }

    # label normalisation: type 3 calls it "Operating expenses"; everyone else
    # "Total operating expenses" — merge to one column in the wide FP table.
    LABEL_ALIASES = {"operating_expenses": "total_operating_expenses"}
