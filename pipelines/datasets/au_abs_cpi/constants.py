"""Constants for the au_abs_cpi (ABS Consumer Price Index, Australia) dataset."""

from enum import Enum


class constants(Enum):
    DATASET_ID = "au_abs_cpi"

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
        "quarterly": [
            "year",
            "quarter",
            "region",
            "serie_id",
            "index_name",
            "index_number",
            "percentage_change_period",
            "percentage_change_year",
        ],
        "monthly": [
            "year",
            "month",
            "region",
            "serie_id",
            "index_name",
            "index_number",
            "percentage_change_period",
            "percentage_change_year",
        ],
    }

    # Sub-annual period column and the year-over-year lag (in periods) per frequency.
    PERIOD_COL = {"quarterly": "quarter", "monthly": "month"}
    YOY_LAG = {"quarterly": 4, "monthly": 12}
