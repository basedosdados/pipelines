"""Constants for the us_bls_cpi recurring pipeline (Prefect 3).

US Consumer Price Index (BLS), CPI-U (`cu`) + CPI-W (`cw`) time.series flat files.
See models/us_bls_cpi/ONBOARDING_PLAN.md for the full design.
"""

from enum import Enum
from pathlib import Path

# Repo root, then the committed architecture CSVs (the single schema source of
# truth — column order + bigquery_type per table).
_REPO_ROOT = Path(__file__).resolve().parents[3]


class constants(Enum):
    DATASET_ID = "us_bls_cpi"

    # download.bls.gov 403s without a browser User-Agent; BLS asks for a contact
    # email in the UA string.
    BASE_URL = "https://download.bls.gov/pub/time.series"
    USER_AGENT = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120 Safari/537.36 rdahis@basedosdados.org"
    )

    # survey dir -> survey label used in the data
    SURVEYS = {"cu": "CPI-U", "cw": "CPI-W"}

    # Small dimension/lookup files pulled per survey.
    DIM_FILES = [
        "area",
        "item",
        "period",
        "seasonal",
        "periodicity",
        "base",
        "series",
    ]
    # Full-history data files (by item/area group). `<s>.data.0.Current` is a
    # recent-only subset already contained here, so it is skipped.
    DATA_GROUPS = [
        "1.AllItems",
        "2.Summaries",
        "3.AsizeNorthEast",
        "4.AsizeNorthCentral",
        "5.AsizeSouth",
        "6.AsizeWest",
        "7.OtherNorthEast",
        "8.OtherNorthCentral",
        "9.OtherSouth",
        "10.OtherWest",
        "11.USFoodBeverage",
        "12.USHousing",
        "13.USApparel",
        "14.USTransportation",
        "15.USMedical",
        "16.USRecreation",
        "17.USEducationAndCommunication",
        "18.USOtherGoodsAndServices",
        "19.PopulationSize",
        "20.USCommoditiesServicesSpecial",
    ]

    # Data tables (partitioned parquet) + the static dictionary.
    DATA_TABLES = ["monthly", "annual", "semiannual"]
    ALL_TABLES = ["monthly", "annual", "semiannual", "dicionario"]

    # Series identity within a survey (used to compute per-series % changes).
    KEYS = ["survey", "seasonal_adjustment", "area_id", "item_id"]

    ARCHITECTURE_DIR = (
        _REPO_ROOT / "models" / "us_bls_cpi" / "code" / "architecture"
    )
