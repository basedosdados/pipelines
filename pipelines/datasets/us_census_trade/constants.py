"""Constants for the us_census_trade recurring pipeline (Prefect 3).

U.S. Census Bureau (Foreign Trade Division) monthly merchandise trade
statistics, pulled from the International Trade timeseries API
(https://api.census.gov/data/timeseries/intltrade/).

Six fact tables at HS6 x partner country x place x month, on three place
dimensions (customs district, port, state), plus a ``dicionario``.

The API key is read from the ``CENSUS_API_KEY`` environment variable when
present (local development) and otherwise from HashiCorp Vault on the deployed
worker -- see ``pipelines.datasets.us_census_trade.utils._key``. Every request
to api.census.gov now requires a key; anonymous requests are redirected to a
"Missing Key" page.
"""

from enum import Enum
from pathlib import Path

# Repo root, then the committed architecture CSVs, which describe the FINAL
# (post-dbt) schema. The raw STAGING schema this pipeline writes is all-STRING
# in the same column order -- see ``utils.write_partitioned``.
_REPO_ROOT = Path(__file__).resolve().parents[3]


class constants(Enum):
    """Constants for the us_census_trade pipeline.

    Lowercase class name follows the repo-wide convention for dataset constant
    enums.
    """

    DATASET_ID = "us_census_trade"

    ARCHITECTURE_DIR = (
        _REPO_ROOT / "models" / "us_census_trade" / "code" / "architecture"
    )

    # Census Schedule C code -> ISO 3166-1 alpha-3, built once from
    # br_bd_diretorios_mundo.pais (which is what country_iso3_code links to)
    # and committed, so the transform needs no BigQuery access at run time.
    ISO3_MAP = Path(__file__).resolve().parent / "country_iso3.json"

    BASE_URL = "https://api.census.gov/data/timeseries/intltrade/"

    # API key resolution. When CENSUS_API_KEY is absent from the environment
    # (the deployed worker), the key is read from Vault at VAULT_SECRET_PATH
    # under VAULT_KEY. That secret must be provisioned before the schedule is
    # armed -- and before any dev run, since the download cannot start without
    # it.
    ENV_KEY = "CENSUS_API_KEY"
    VAULT_SECRET_PATH = "us_census_trade"
    VAULT_KEY = "CENSUS_API_KEY"

    # Min seconds between call starts. Census publishes no documented rate
    # limit for keyed requests; this is deliberate politeness, not a measured
    # ceiling.
    MIN_INTERVAL = 0.4

    # Census: "statistics from January 2010 to present".
    FIRST_YEAR = 2010
    FIRST_MONTH = "2010-01"

    # Commodity aggregation level requested from the API. The API returns HS2,
    # HS4, HS6 and HS10 rows in the same response, so this is filtered both as
    # a predicate and again client-side; without it every total is inflated
    # several-fold.
    COMM_LVL = "HS6"

    # Detail rows only. 'CGP' rows are country GROUPINGS (OPEC, the European
    # Union, ...) that aggregate the detail rows; keeping them would double
    # count every member country.
    SUMMARY_LVL = "DET"

    # Refresh window, in years back from the newest month, inclusive.
    #
    # Census revises year-to-date months at every release AND revises all
    # previously released data with the publication of April statistics. A
    # window of the current year plus the previous one covers both without
    # special-casing April.
    REFRESH_YEARS_BACK = 1

    # WCO Harmonized System revisions, as adopted by the United States. The
    # code space is NOT continuous across these boundaries.
    HS_REVISIONS = (
        (2010, 2011, "HS2007"),
        (2012, 2016, "HS2012"),
        (2017, 2021, "HS2017"),
        (2022, 9999, "HS2022"),
    )

    # Public Census code schedules. No API key needed for these.
    SCHEDULE_C_URL = (
        "https://www.census.gov/foreign-trade/schedules/c/country.txt"
    )
    SCHEDULE_D_URL = (
        "https://www.census.gov/foreign-trade/schedules/d/dist.txt"
    )

    # www.census.gov serves these only to a browser user agent.
    HTTP_HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"
        )
    }

    # Tokens the API uses for missing values -> NULL.
    MISSING_TOKENS = ("", "-", "(D)", "(X)", "N/A", "NA", "null", "None")
