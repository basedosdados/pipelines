"""Constants for the us_eia_consumption recurring pipeline (Prefect 3).

U.S. Energy Information Administration **Form EIA-861** — the retail (demand)
side of the electric power industry: electricity sold to end-use customers, with
revenue, sales and customer counts by utility, state and customer sector, plus
the utility frame and county-level service territory; and the monthly **Form
EIA-861M**, the state-by-sector monthly counterpart.

See ``models/us_eia_consumption/CLAUDE.md`` for the full design, including the
three annual-file layout eras and what was deferred.

Tables:

* ``utility`` — one row per utility per year (name, ownership, state, NERC region)
* ``retail_sales`` — utility x year x state x service type x customer sector,
  with sales, revenue, customers and a derived average price
* ``service_territory`` — utility x year x county (2012 on, when EIA began
  publishing it as its own file)
* ``eia861m`` — state x year x month x customer sector, monthly (the BD Pro table)
"""

from enum import Enum
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[3]
_CODE_DIR = _REPO_ROOT / "models" / "us_eia_consumption" / "code"


class constants(Enum):
    """Constants for the us_eia_consumption pipeline."""

    DATASET_ID = "us_eia_consumption"

    UTILITY = "utility"
    RETAIL_SALES = "retail_sales"
    SERVICE_TERRITORY = "service_territory"
    EIA861M = "eia861m"
    DICIONARIO = "dicionario"
    # Every table is built before any is tested (the data tables'
    # custom_dictionary_coverage test reads dicionario).
    ALL_TABLES = [
        "utility",
        "retail_sales",
        "service_territory",
        "eia861m",
        "dicionario",
    ]
    DATA_TABLES = ["utility", "retail_sales", "service_territory", "eia861m"]
    # Tables built from the annual EIA-861 ZIPs (as opposed to the 861M file).
    ANNUAL_TABLES = ["utility", "retail_sales", "service_territory"]

    ARCHITECTURE_DIR = _CODE_DIR / "architecture"
    COUNTY_DIRECTORY = _CODE_DIR / "us_county_directory.csv"

    # 2001 is the floor for the annual utility-level tables: before it EIA-861 is
    # a fixed record-type survey (F861TYPn.xls) with no by-sector utility grain,
    # a differently shaped form deferred as a clean follow-up (the same reason
    # us_eia_electricity floored at 2001). service_territory begins in 2012, when
    # EIA first published it as its own file. eia861m carries its own full
    # 1990-present history from a single harmonised file.
    FIRST_ANNUAL_YEAR = 2001
    SERVICE_TERRITORY_FIRST_YEAR = 2012
    EIA861M_FIRST_YEAR = 1990

    EIA861_PAGE_URL = "https://www.eia.gov/electricity/data/eia861/"
    EIA861M_PAGE_URL = "https://www.eia.gov/electricity/data/eia861m/"
    EIA861M_XLS_URL = (
        "https://www.eia.gov/electricity/data/eia861m/xls/sales_revenue.xlsx"
    )
    # The current sales_revenue.xlsx Monthly sheet only reaches back to 2010; the
    # 1990-2009 history lives in a separate archived workbook with the same
    # banner layout (its fifth sector is OTHER, a real residual sector, where the
    # current file's fifth column is a TOTAL that is dropped).
    EIA861M_HISTORICAL_URL = "https://www.eia.gov/electricity/data/eia861m/archive/xls/HS861M%201990-2009.xlsx"

    USER_AGENT = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    )

    # Candidate URL templates for a report year's annual ZIP, tried in order.
    # EIA renames the current year's file on every release, so the settled years
    # are exact but the live year needs the early-release and 2-digit fallbacks.
    # A member inside is then resolved by pattern, never by a literal name.
    ZIP_URL_TEMPLATES = [
        "https://www.eia.gov/electricity/data/eia861/zip/f861{yyyy}.zip",
        "https://www.eia.gov/electricity/data/eia861/zip/f861{yyyy}er.zip",
        "https://www.eia.gov/electricity/data/eia861/archive/zip/f861{yyyy}.zip",
        "https://www.eia.gov/electricity/data/eia861/archive/zip/f861{yy}.zip",
    ]

    # Per-table regex to find the right workbook inside a year's ZIP, tried in
    # order and matched case-insensitively against the member's base name with
    # {year} substituted. A pattern matching no member, or more than one, raises
    # rather than guessing. Ordered so the era-specific name wins first.
    #
    # file1 = utility frame (2001-2011); file2 = sales to ultimate customers
    # (2001-2011). Modern (2012+) names them Utility_Data / Sales_Ult_Cust /
    # Service_Territory. The trailing ``$`` and the exclusion of ``_cao`` / ``_a``
    # / ``_cs`` keep sibling files from matching.
    MEMBER_PATTERNS = {
        "utility": [
            r"utility_data_{year}\.xlsx?$",
            r"(^|/)file1_{year}\.xlsx?$",
            r"(^|/)file1\.xls$",
        ],
        "retail_sales": [
            r"sales_ult_cust_{year}\.xlsx?$",
            r"(^|/)file2_{year}\.xlsx?$",
            r"(^|/)file2\.xls$",
        ],
        "service_territory": [
            r"service_territory_{year}\.xlsx?$",
        ],
    }

    # The end-use customer sectors melted out of the wide sector blocks. TOTAL is
    # dropped (it is the sum of the others); OTHER is kept — it is a real residual
    # sector that appears in the 1990-2009 monthly history, not a total.
    SECTORS = [
        "residential",
        "commercial",
        "industrial",
        "transportation",
        "other",
    ]

    # Raw sector banner label (normalised) -> sector slug.
    SECTOR_BANNER = {
        "residential": "residential",
        "res": "residential",
        "commercial": "commercial",
        "com": "commercial",
        "industrial": "industrial",
        "ind": "industrial",
        "transportation": "transportation",
        "trans": "transportation",
        "other": "other",
        "total": "total",
    }
