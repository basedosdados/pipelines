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

    # ----------------------------------------------------------------- #
    # The other ABS releases in the topic, one fact table each
    # ----------------------------------------------------------------- #
    RELEASE_LANDING_URL = (
        "https://www.abs.gov.au/statistics/economy/price-indexes-and-inflation/"
        "{path}/latest-release"
    )
    RELEASE_FILE_URL = (
        "https://www.abs.gov.au/statistics/economy/price-indexes-and-inflation/"
        "{path}/{slug}/{file}.xlsx"
    )

    # `file_re` keeps only the time-series workbooks. It drops the PPI/ITPI data
    # cubes (`64270do001_202606`, `64570DO001`), which are a different layout,
    # and the WPI `63450Table2ato9a`/`2bto9b` consolidations, whose every series
    # also appears in the individual table workbooks (verified: 648 either way).
    RELEASES = {
        "wage_price_index": {
            "catalogue": "6345.0",
            "path": "wage-price-index-australia",
            "slug_re": r"wage-price-index-australia/([a-z]{3}-\d{4})/",
            "file_re": r"^63450\d+[ab]?$",
        },
        "producer_price_index": {
            "catalogue": "6427.0",
            "path": "producer-price-indexes-australia",
            "slug_re": r"producer-price-indexes-australia/([a-z]{3}-\d{4})/",
            "file_re": r"^64270\d+$",
        },
        "international_trade_price_index": {
            "catalogue": "6457.0",
            "path": "international-trade-price-indexes-australia",
            "slug_re": (
                r"international-trade-price-indexes-australia/([a-z]{3}-\d{4})/"
            ),
            "file_re": r"^64570\d+$",
        },
        "living_cost_index": {
            "catalogue": "6467.0",
            "path": "selected-living-cost-indexes-australia",
            "slug_re": (
                r"selected-living-cost-indexes-australia/([a-z]{3}-\d{4})/"
            ),
            "file_re": r"^64670\d+$",
        },
        "dwelling_value": {
            "catalogue": "6432.0",
            "path": "total-value-dwellings",
            "slug_re": r"total-value-dwellings/([a-z]{3}-quarter-\d{4})/",
            "file_re": r"^64320\d+$",
        },
    }

    # Canonical statistic labels. ABS spells the same statistic differently
    # between a release's quarterly and financial-year workbooks ("Percentage
    # Change from Previous Quarter" vs "... Previous Period"), and varies the
    # case ("from" vs "From"), so the raw label is normalised before matching or
    # one statistic silently becomes two.
    STATISTIC = {
        "index": "Index number",
        "period": "Percentage change from previous period",
        "year": "Percentage change from corresponding period of previous year",
        "points": "Points contribution to All groups",
        "points_change": "Change in points contribution",
    }

    # Wage Price Index vocabularies. Its description parts are NOT in a fixed
    # order -- position 1 carries the pay measure or a region, position 2 a
    # region, a sector or a pay measure -- so each part is classified by shape
    # against these sets instead of being read by position.
    WPI_REGIONS = {
        "Australia",
        "New South Wales",
        "Victoria",
        "Queensland",
        "South Australia",
        "Western Australia",
        "Tasmania",
        "Northern Territory",
        "Australian Capital Territory",
    }
    WPI_SECTORS = {"Private", "Public", "Private and Public"}
    # Normalised (lowercased, "time"/"hourly" dropped) -> canonical label.
    WPI_PAY_MEASURE = {
        "total rates of pay excluding bonuses": (
            "Total hourly rates of pay excluding bonuses"
        ),
        "ordinary rates of pay excluding bonuses": (
            "Ordinary time hourly rates of pay excluding bonuses"
        ),
        "total rates of pay including bonuses": (
            "Total hourly rates of pay including bonuses"
        ),
        "ordinary rates of pay including bonuses": (
            "Ordinary time hourly rates of pay including bonuses"
        ),
    }

    # International Trade classification, keyed on a fragment of the ABS table
    # title. The import/export direction and the classification system appear
    # ONLY in the title -- the description omits both, so "All groups" collides
    # six ways without them.
    ITPI_CLASSIFICATION = {
        "Broad Economic Categories": "Balance of Payments BEC",
        "Balance of Payments classification": "Balance of Payments",
        "BEC Category": "BEC",
        "HTISC": "HTISC",
        "ANZSIC": "ANZSIC",
        "AHECC": "AHECC",
        "SITC": "SITC",
    }

    # Column order per output table (matches the architecture CSVs).
    RELEASE_COLUMNS = {
        "wage_price_index": [
            "year",
            "quarter",
            "financial_year",
            "series_id",
            "statistic",
            "series_type",
            "pay_measure",
            "region",
            "sector",
            "industry",
            "frequency",
            "unit",
            "value",
        ],
        "producer_price_index": [
            "year",
            "quarter",
            "series_id",
            "statistic",
            "index_type",
            "item_code",
            "item_name",
            "region",
            "source_table",
            "unit",
            "value",
        ],
        "international_trade_price_index": [
            "year",
            "quarter",
            "series_id",
            "statistic",
            "index_type",
            "classification",
            "item_code",
            "item_name",
            "source_table",
            "unit",
            "value",
        ],
        "living_cost_index": [
            "year",
            "quarter",
            "series_id",
            "statistic",
            "household_type",
            "commodity_group",
            "unit",
            "value",
        ],
        "dwelling_value": [
            "year",
            "quarter",
            "series_id",
            "measure",
            "owner_sector",
            "region",
            "unit",
            "value",
        ],
    }
