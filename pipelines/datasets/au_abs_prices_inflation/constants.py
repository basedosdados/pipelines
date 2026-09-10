"""Constants for the au_abs_prices_inflation dataset (ABS price indexes).

The dataset covers the whole ABS "Price indexes and inflation" topic, one fact
table per ABS release. This module holds the shared settings plus the
Consumer Price Index (former catalogue 6401.0) release configuration.
"""

from enum import Enum


class constants(Enum):
    DATASET_ID = "au_abs_prices_inflation"

    # ABS time-series workbook location.
    # Release slug is the latest reference period, e.g. "jun-2026".
    BASE_URL = (
        "https://www.abs.gov.au/statistics/economy/price-indexes-and-inflation/"
        "consumer-price-index-australia/{slug}/{file}.xlsx"
    )
    LANDING_URL = (
        "https://www.abs.gov.au/statistics/economy/price-indexes-and-inflation/"
        "consumer-price-index-australia/latest-release"
    )
    # download.abs.gov.au / www.abs.gov.au reject the default requests UA.
    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"
        )
    }

    # Which ABS tables feed which output table. Filename stem -> nothing else needed;
    # every table is parsed the same way and split by its declared frequency.
    SOURCE_TABLES = {
        "quarterly": ["6401017", "6401018"],
        "monthly": ["640101", "640103", "6401010"],
    }

    # CPI release frequency -> output table slug. The dataset holds one fact
    # table per ABS release, so the CPI tables carry the release prefix while
    # the frequency keys above stay semantic (they drive PERIOD_COL/YOY_LAG).
    TABLE_ID = {"quarterly": "cpi_quarterly", "monthly": "cpi_monthly"}

    # Measures we keep, normalised to output column names. Everything else
    # (Contribution, Change in Contribution, ...) is dropped.
    MEASURE_MAP = {
        "Index Numbers": "index_number",
        "Percentage Change from Previous Period": "percentage_change_period",
        "Percentage Change from Corresponding Month of Previous Year": "percentage_change_year",
        "Percentage Change from Corresponding Quarter of Previous Year": "percentage_change_year",
    }

    # Column order per output table (matches architecture CSVs).
    COLUMNS = {
        "cpi_quarterly": [
            "year",
            "quarter",
            "region",
            "index_code",
            "index_name",
            "serie_id",
            "index_number",
            "percentage_change_period",
            "percentage_change_year",
        ],
        "cpi_monthly": [
            "year",
            "month",
            "region",
            "index_code",
            "index_name",
            "serie_id",
            "index_number",
            "percentage_change_period",
            "percentage_change_year",
        ],
    }

    # Item ID map file (index_name -> ABS CL_CPI_INDEX code), bundled beside cpi.py.
    INDEX_CODES_FILE = "index_codes.csv"

    # Sub-annual period column and the year-over-year lag (in periods) per frequency.
    PERIOD_COL = {"quarterly": "quarter", "monthly": "month"}
    YOY_LAG = {"quarterly": 4, "monthly": 12}
