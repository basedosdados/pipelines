"""Constants and column maps for the us_census_lodes dataset.

Source: LEHD Origin-Destination Employment Statistics (LODES), version 8 (LODES8),
U.S. Census Bureau. https://lehd.ces.census.gov/data/lodes/LODES8/

Shared by the one-shot onboarding bootstrap (``models/us_census_lodes/code/``)
and the recurring Prefect pipeline in this package, so the schema is defined
once.

Everything here is derived from LODESTechDoc8.4.pdf, which is the authoritative
data dictionary. Do not invent column meanings; look them up there.

Scope decisions (see ONBOARDING_PLAN.md for the measurements behind them):
  * RAC and WAC are shipped in the published WIDE shape, S000 segment only,
    all six job types, 2002-2023.
  * OD is deliberately out of scope for this onboarding.
"""

from __future__ import annotations

import os
from pathlib import Path

BASE_URL = "https://lehd.ces.census.gov/data/lodes/LODES8"
DATASET_ID = "us_census_lodes"

# Repo root, then the committed architecture CSVs (the single schema source of
# truth -- column order + bigquery_type per table).
_REPO_ROOT = Path(__file__).resolve().parents[3]
ARCHITECTURE_DIR = _REPO_ROOT / "models" / DATASET_ID / "code" / "architecture"

# Scratch data never lives in the repo or under Dropbox.
DATA_ROOT = Path(
    os.environ.get(
        "LODES_DATA_ROOT", Path.home() / "Downloads" / "us_census_lodes_data"
    )
)
INPUT = DATA_ROOT / "input"
OUTPUT = DATA_ROOT / "output"

# 50 states + DC + Puerto Rico. "us" holds only a national crosswalk and is
# excluded: its blocks are the union of the state files.
STATES = [
    "ak",
    "al",
    "ar",
    "az",
    "ca",
    "co",
    "ct",
    "dc",
    "de",
    "fl",
    "ga",
    "hi",
    "ia",
    "id",
    "il",
    "in",
    "ks",
    "ky",
    "la",
    "ma",
    "md",
    "me",
    "mi",
    "mn",
    "mo",
    "ms",
    "mt",
    "nc",
    "nd",
    "ne",
    "nh",
    "nj",
    "nm",
    "nv",
    "ny",
    "oh",
    "ok",
    "or",
    "pa",
    "pr",
    "ri",
    "sc",
    "sd",
    "tn",
    "tx",
    "ut",
    "va",
    "vt",
    "wa",
    "wi",
    "wv",
    "wy",
]

YEARS = list(range(2002, 2024))
JOB_TYPES = ["JT00", "JT01", "JT02", "JT03", "JT04", "JT05"]

# Only the "all workers" segment is shipped. The other nine segments (SA01-03,
# SE01-03, SI01-03) repeat the same column grid restricted to a worker subset,
# i.e. they carry two-way interactions. Shipping them would multiply both tables
# by ten for a marginal gain; the file layout is documented so they can be added
# later behind a `segment` column.
SEGMENT = "S000"

JOB_TYPE_LABELS = {
    "JT00": ("All Jobs", "Todos os empregos", "Todos los empleos"),
    "JT01": ("Primary Jobs", "Empregos principais", "Empleos principales"),
    "JT02": (
        "All Private Jobs",
        "Todos os empregos privados",
        "Todos los empleos privados",
    ),
    "JT03": (
        "Private Primary Jobs",
        "Empregos principais privados",
        "Empleos principales privados",
    ),
    "JT04": (
        "All Federal Jobs",
        "Todos os empregos federais",
        "Todos los empleos federales",
    ),
    "JT05": (
        "Federal Primary Jobs",
        "Empregos principais federais",
        "Empleos principales federales",
    ),
}

# --------------------------------------------------------------------------
# RAC / WAC value columns: source code -> destination name.
# Order here IS the architecture order for the count block.
# --------------------------------------------------------------------------

# The 41 columns common to RAC and WAC.
SHARED_COUNTS: list[tuple[str, str]] = [
    ("C000", "jobs_total"),
    # Age
    ("CA01", "jobs_age_29_or_younger"),
    ("CA02", "jobs_age_30_to_54"),
    ("CA03", "jobs_age_55_or_older"),
    # Monthly earnings
    ("CE01", "jobs_earnings_1250_or_less"),
    ("CE02", "jobs_earnings_1251_to_3333"),
    ("CE03", "jobs_earnings_above_3333"),
    # NAICS sector. Named by the sector code, not the CNSnn ordinal, so the
    # column is readable without the tech doc.
    ("CNS01", "jobs_naics_11"),
    ("CNS02", "jobs_naics_21"),
    ("CNS03", "jobs_naics_22"),
    ("CNS04", "jobs_naics_23"),
    ("CNS05", "jobs_naics_31_33"),
    ("CNS06", "jobs_naics_42"),
    ("CNS07", "jobs_naics_44_45"),
    ("CNS08", "jobs_naics_48_49"),
    ("CNS09", "jobs_naics_51"),
    ("CNS10", "jobs_naics_52"),
    ("CNS11", "jobs_naics_53"),
    ("CNS12", "jobs_naics_54"),
    ("CNS13", "jobs_naics_55"),
    ("CNS14", "jobs_naics_56"),
    ("CNS15", "jobs_naics_61"),
    ("CNS16", "jobs_naics_62"),
    ("CNS17", "jobs_naics_71"),
    ("CNS18", "jobs_naics_72"),
    ("CNS19", "jobs_naics_81"),
    ("CNS20", "jobs_naics_92"),
    # Race (2009+)
    ("CR01", "jobs_race_white"),
    ("CR02", "jobs_race_black"),
    ("CR03", "jobs_race_american_indian_alaska_native"),
    ("CR04", "jobs_race_asian"),
    ("CR05", "jobs_race_native_hawaiian_pacific_islander"),
    ("CR07", "jobs_race_two_or_more"),
    # Ethnicity (2009+)
    ("CT01", "jobs_ethnicity_not_hispanic"),
    ("CT02", "jobs_ethnicity_hispanic"),
    # Educational attainment, workers 30+ only (2009+)
    ("CD01", "jobs_education_less_than_high_school"),
    ("CD02", "jobs_education_high_school"),
    ("CD03", "jobs_education_some_college"),
    ("CD04", "jobs_education_bachelors_or_higher"),
    # Sex (2009+)
    ("CS01", "jobs_sex_male"),
    ("CS02", "jobs_sex_female"),
]

# WAC only: firm age and firm size (2011+, and only for JT02 All Private Jobs).
FIRM_COUNTS: list[tuple[str, str]] = [
    ("CFA01", "jobs_firm_age_0_to_1"),
    ("CFA02", "jobs_firm_age_2_to_3"),
    ("CFA03", "jobs_firm_age_4_to_5"),
    ("CFA04", "jobs_firm_age_6_to_10"),
    ("CFA05", "jobs_firm_age_11_or_more"),
    ("CFS01", "jobs_firm_size_0_to_19"),
    ("CFS02", "jobs_firm_size_20_to_49"),
    ("CFS03", "jobs_firm_size_50_to_249"),
    ("CFS04", "jobs_firm_size_250_to_499"),
    ("CFS05", "jobs_firm_size_500_or_more"),
]

RAC_COUNTS = SHARED_COUNTS
WAC_COUNTS = SHARED_COUNTS + FIRM_COUNTS

# Geographic key columns, in architecture order. `year` is the partition;
# state/county/tract are prefixes of the block code, materialised so the table
# can be clustered and joined to br_bd_diretorios_us without a substring.
GEO_KEYS = ["year", "state_id", "county_id", "census_tract_id", "block_id"]


def rac_columns() -> list[str]:
    return (
        GEO_KEYS + ["job_type"] + [d for _, d in RAC_COUNTS] + ["date_created"]
    )


def wac_columns() -> list[str]:
    return (
        GEO_KEYS + ["job_type"] + [d for _, d in WAC_COUNTS] + ["date_created"]
    )


# --------------------------------------------------------------------------
# Geography crosswalk: source -> destination. One row per 2020 tabulation block.
# --------------------------------------------------------------------------
XWALK_COLUMNS: list[tuple[str, str]] = [
    ("tabblk2020", "block_id"),
    ("st", "state_id"),
    ("stusps", "state_abbreviation"),
    ("stname", "state_name"),
    ("cty", "county_id"),
    ("ctyname", "county_name"),
    ("trct", "census_tract_id"),
    ("trctname", "census_tract_name"),
    ("bgrp", "block_group_id"),
    ("bgrpname", "block_group_name"),
    ("cbsa", "cbsa_id"),
    ("cbsaname", "cbsa_name"),
    ("zcta", "zcta_id"),
    ("zctaname", "zcta_name"),
    ("stplc", "place_id"),
    ("stplcname", "place_name"),
    ("ctycsub", "county_subdivision_id"),
    ("ctycsubname", "county_subdivision_name"),
    ("stcd119", "congressional_district_id"),
    ("stcd119name", "congressional_district_name"),
    ("stsldl", "state_legislative_district_lower_id"),
    ("stsldlname", "state_legislative_district_lower_name"),
    ("stsldu", "state_legislative_district_upper_id"),
    ("stslduname", "state_legislative_district_upper_name"),
    ("stschool", "school_district_id"),
    ("stschoolname", "school_district_name"),
    ("stsecon", "secondary_school_district_id"),
    ("stseconname", "secondary_school_district_name"),
    ("trib", "tribal_area_id"),
    ("tribname", "tribal_area_name"),
    ("tsub", "tribal_subdivision_id"),
    ("tsubname", "tribal_subdivision_name"),
    ("stanrc", "alaska_native_corporation_id"),
    ("stanrcname", "alaska_native_corporation_name"),
    ("mil", "military_installation_id"),
    ("milname", "military_installation_name"),
    ("stwib", "workforce_board_id"),
    ("stwibname", "workforce_board_name"),
    ("blklatdd", "latitude"),
    ("blklondd", "longitude"),
    ("createdate", "date_created"),
]


def xwalk_columns() -> list[str]:
    return [d for _, d in XWALK_COLUMNS]


def is_sentinel(code: str) -> bool:
    """True for the crosswalk's 'not applicable' fill.

    LODES pads inapplicable geographies with an all-nines code of the column's
    own width (99999, 9999999, 9999999999999999999999, ...). A genuine code is
    never all nines. Empty strings are handled by the caller.
    """
    return bool(code) and set(code) == {"9"}


def rac_url(state: str, job_type: str, year: int) -> str:
    return f"{BASE_URL}/{state}/rac/{state}_rac_{SEGMENT}_{job_type}_{year}.csv.gz"


def wac_url(state: str, job_type: str, year: int) -> str:
    return f"{BASE_URL}/{state}/wac/{state}_wac_{SEGMENT}_{job_type}_{year}.csv.gz"


def xwalk_url(state: str) -> str:
    return f"{BASE_URL}/{state}/{state}_xwalk.csv.gz"
