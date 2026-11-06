"""Constants for us_usda_nass (USDA NASS QuickStats + Census of Agriculture).

Curated, robustness-first onboarding: the 5 QuickStats bulk sector files are the
whole database (39 tab-separated columns, ``SOURCE_DESC in {CENSUS, SURVEY}``).
Both output tables are a ``SOURCE_DESC`` filter on the same files, restricted to a
high-value seed of commodities/statistics/geographies. See
``models/us_usda_nass/code/SEED_EXCLUSIONS.md`` for exactly what is excluded and
``models/us_usda_nass/ONBOARDING_PLAN.md`` for the design.
"""

from enum import Enum
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[3]


class constants(Enum):
    """Constants for the us_usda_nass pipeline (lowercase per repo convention)."""

    DATASET_ID = "us_usda_nass"

    # The bulk-download index and the 5 sector files (date-stamped in the name;
    # resolved at download time by scraping the index). These 5 files together
    # are the entire QuickStats database, both SURVEY and CENSUS source rows.
    DATASETS_INDEX = "https://www.nass.usda.gov/datasets/"
    SECTORS = [
        "animals_products",
        "crops",
        "demographics",
        "economics",
        "environmental",
    ]
    # NASS serves the bulk files to plain clients, but send a contact UA anyway.
    USER_AGENT = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120 Safari/537.36 rdahis@basedosdados.org"
    )

    # Output tables — split by geography grain (one grain per table, per-grain
    # schema). SURVEY -> survey_{national,state,agricultural_district,county};
    # CENSUS -> census_of_agriculture_{national,state,county} (census has no
    # agricultural-district grain in QuickStats). `dicionario` is shared.
    FACT_TABLES = [
        "survey_national",
        "survey_state",
        "survey_agricultural_district",
        "survey_county",
        "census_of_agriculture_national",
        "census_of_agriculture_state",
        "census_of_agriculture_county",
    ]
    ALL_TABLES = [
        "survey_national",
        "survey_state",
        "survey_agricultural_district",
        "survey_county",
        "census_of_agriculture_national",
        "census_of_agriculture_state",
        "census_of_agriculture_county",
        "dicionario",
    ]

    # ---- Seed filter (LOG every exclusion in SEED_EXCLUSIONS.md) ----
    SEED_COMMODITIES = frozenset(
        {
            "CORN",
            "SOYBEANS",
            "WHEAT",
            "COTTON",
            "SORGHUM",
            "RICE",
            "HAY",
            "BARLEY",
            "OATS",
            "PEANUTS",
            "CATTLE",
            "HOGS",
            "MILK",
            "CHICKENS",
            "EGGS",
            "TURKEYS",
        }
    )
    SEED_GEO_LEVELS = frozenset(
        {"NATIONAL", "STATE", "COUNTY", "AGRICULTURAL DISTRICT"}
    )
    SEED_FREQ = frozenset({"ANNUAL"})
    # statisticcat_desc is compound ("INVENTORY OF MILK COWS", "SALES OF HOGS"),
    # so match the headline set by PREFIX, not equality.
    SEED_STAT_PREFIXES = (
        "PRODUCTION",
        "YIELD",
        "AREA PLANTED",
        "AREA HARVESTED",
        "AREA BEARING",
        "AREA NON-BEARING",
        "AREA GROWN",
        "PRICE RECEIVED",
        "STOCKS",
        "INVENTORY",
        "SALES",
    )
    # No domain filter: all domains kept (census cross-tabs are the census value).

    # ---- Raw QuickStats layout: 39 tab-separated UPPERCASE columns ----
    RAW_COLUMNS = [
        "SOURCE_DESC",
        "SECTOR_DESC",
        "GROUP_DESC",
        "COMMODITY_DESC",
        "CLASS_DESC",
        "PRODN_PRACTICE_DESC",
        "UTIL_PRACTICE_DESC",
        "STATISTICCAT_DESC",
        "UNIT_DESC",
        "SHORT_DESC",
        "DOMAIN_DESC",
        "DOMAINCAT_DESC",
        "AGG_LEVEL_DESC",
        "STATE_ANSI",
        "STATE_FIPS_CODE",
        "STATE_ALPHA",
        "STATE_NAME",
        "ASD_CODE",
        "ASD_DESC",
        "COUNTY_ANSI",
        "COUNTY_CODE",
        "COUNTY_NAME",
        "REGION_DESC",
        "ZIP_5",
        "WATERSHED_CODE",
        "WATERSHED_DESC",
        "CONGR_DISTRICT_CODE",
        "COUNTRY_CODE",
        "COUNTRY_NAME",
        "LOCATION_DESC",
        "YEAR",
        "FREQ_DESC",
        "BEGIN_CODE",
        "END_CODE",
        "REFERENCE_PERIOD_DESC",
        "WEEK_ENDING",
        "LOAD_TIME",
        "VALUE",
        "CV_%",
    ]

    # Value suppression sentinels -> the flag captured in value_suppression_flag.
    # Any value that starts with "(" is treated as a suppression code and mapped
    # to NULL; these are the documented ones (also written to the dicionario).
    SUPPRESSION_CODES = {
        "(D)": "Retido para evitar divulgar dados de operações individuais",
        "(Z)": "Menos da metade da unidade de arredondamento",
        "(X)": "Não aplicável",
        "(S)": "Suprimido por insuficiência de dados / confiabilidade",
        "(H)": "Coeficiente de variação ou erro padrão alto",
        "(L)": "Coeficiente de variação ou erro padrão baixo",
        "(NA)": "Não disponível",
    }
    SUPPRESSION_LABELS_EN = {
        "(D)": "Withheld to avoid disclosing data for individual operations",
        "(Z)": "Less than half the rounding unit",
        "(X)": "Not applicable",
        "(S)": "Suppressed for insufficient data or reliability",
        "(H)": "High coefficient of variation or standard error",
        "(L)": "Low coefficient of variation or standard error",
        "(NA)": "Not available",
    }

    ARCHITECTURE_DIR = (
        _REPO_ROOT / "models" / "us_usda_nass" / "code" / "architecture"
    )
