"""Constants for the us_bls_cpi recurring pipeline (Prefect 3).

US Consumer Price Index (BLS), CPI-U (`cu`) + CPI-W (`cw`) time.series flat files.
See models/us_bls_cpi/ONBOARDING_PLAN.md for the full design.
"""

from enum import Enum
from pathlib import Path

from pipelines.utils.metadata.domain import (
    AllFree,
    DateFormat,
    FreeLag,
    NonHistorical,
    PartBdpro,
    YearMonth,
    YearOnly,
)

# Repo root, then the committed architecture CSVs (the single schema source of
# truth — column order + bigquery_type per table).
_REPO_ROOT = Path(__file__).resolve().parents[3]


class constants(Enum):
    """Constants for the us_bls_cpi pipeline.

    Lowercase class name follows the repo-wide convention for dataset constant
    enums. ``ARCHITECTURE_DIR`` points at the architecture CSVs under
    ``models/us_bls_cpi/code/``, which are the schema source of truth for both
    this pipeline and the one-shot bootstrap.
    """

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


# Coverage spec per table, used by `ExtractAndLoad.coverage` (tasks.py) and,
# through it, by `register_table_materialization_task`/`build_and_promote`.
#
# `monthly` is the high-frequency table, so it carries the BD Pro rolling
# window: the most recent 6 months are pro-only, everything older is free.
# Each run recomputes free_end = source_end - free_lag, rewrites both
# DateTimeRanges, and re-issues the BigQuery Row Access Policies, so the
# window slides forward on its own.
#
# `annual` and `semiannual` are lower-frequency and stay fully free.
# `dicionario` has no date column at all (no `year=` partitions either — see
# `utils.build_dicionario`), so it takes `NonHistorical` (modeled on
# `__TABLES__.last_modified_time`, not a `DateColumn`/`DateFormat` pair) — same
# situation as `us_bea`/`au_geoscape_gnaf`'s `dicionario`.
COVERAGE = {
    "monthly": PartBdpro(
        date_column=YearMonth(year="year", month="month"),
        date_format=DateFormat.YEAR_MONTH,
        free_lag=FreeLag(unit="months", value=6),
    ),
    "annual": AllFree(
        date_column=YearOnly(col="year"), date_format=DateFormat.YEAR
    ),
    "semiannual": AllFree(
        date_column=YearOnly(col="year"), date_format=DateFormat.YEAR
    ),
    "dicionario": NonHistorical(),
}
