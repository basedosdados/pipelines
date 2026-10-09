"""Constants for the us_bls_oes recurring pipeline (Prefect 3).

US Occupational Employment and Wage Statistics (BLS). One annual release per
year with a May reference period, published as zipped Excel workbooks under
https://www.bls.gov/oes/special-requests/.

See models/us_bls_oes/ONBOARDING_PLAN.md for the full design.
"""

from enum import Enum
from pathlib import Path

# Repo root, then the committed architecture CSVs (the single schema source of
# truth — column order + bigquery_type per table).
_REPO_ROOT = Path(__file__).resolve().parents[3]
_CODE_DIR = _REPO_ROOT / "models" / "us_bls_oes" / "code"


class constants(Enum):
    """Constants for the us_bls_oes pipeline.

    Lowercase class name follows the repo-wide convention for dataset constant
    enums.
    """

    DATASET_ID = "us_bls_oes"

    # www.bls.gov 403s without a browser User-Agent; BLS asks for a contact
    # email in the UA string.
    BASE_URL = "https://www.bls.gov/oes/special-requests"
    TABLES_URL = "https://www.bls.gov/oes/tables.htm"
    USER_AGENT = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120 Safari/537.36 rdahis@basedosdados.org"
    )

    # First year of the panel. 1997-2002 are excluded: 1997-1999 use the pre-SOC
    # OES occupational taxonomy, and 2000-2002 need per-year header handling.
    FIRST_YEAR = 2003
    # From this year BLS ships one stacked `all` workbook per year; before it,
    # the same estimates are split across nat/st/ma/in4 zips.
    FIRST_ALL_YEAR = 2011
    # From this year metropolitan areas use 5-digit CBSA codes. 2003-2004 use
    # the pre-2003 OMB 4-digit MSA/PMSA codes, which are a different system.
    FIRST_CBSA_YEAR = 2005

    DATA_TABLES = ["area", "industry"]
    ALL_TABLES = ["area", "industry", "dicionario"]

    # A row is an area (cross-industry) estimate iff its NAICS code is one of
    # these OEWS cross-industry pseudo-codes. Verified equivalent to
    # `i_group LIKE 'cross-industry%'` on May 2014 and May 2025, and 1:1 with
    # own_code — which is why the area table carries no industry code.
    CROSS_INDUSTRY_OWNERSHIP = {
        "000000": "1235",
        "000001": "5",
        "999001": "123",
        "999101": "1",
        "999201": "2",
        "999301": "3",
    }

    # OEWS encodes ten ownership universes as pseudo-NAICS codes, each of which
    # determines the ownership. `ownership_id` is derived from these rather than
    # read from `own_code`, because the May 2012 release publishes `own_code = 5`
    # (Private) on every one of them — which would both mislabel the government
    # and cross-industry totals as private and collapse 000000 and 000001 onto
    # the same area-table key. Every other release agrees with the mapping below,
    # and the cleaning run logs any row where the published code disagrees.
    #
    # The `...001/101/201/301` codes include government schools and hospitals and
    # are cross-industry (area table); the `...000/100/200/300` codes exclude
    # them and are industries in their own right (industry table).
    PSEUDO_NAICS_OWNERSHIP = {
        "000000": "1235",
        "000001": "5",
        "999000": "123",
        "999001": "123",
        "999100": "1",
        "999101": "1",
        "999200": "2",
        "999201": "2",
        "999300": "3",
        "999301": "3",
    }

    # 2003-2010 files carry an ownership title instead of a code.
    OWNERSHIP_TITLE_TO_CODE = {
        "private": "5",
        "state government": "2",
        "state government, including schools and hospitals": "2",
        "local government": "3",
        "local government, including schools and hospitals": "3",
        "federal government": "1",
        "federal government, including usps": "1",
        "local, state, and federal government": "123",
        "public sector -- federal, state, and local government, including "
        "government owned schools and hospitals": "123",
    }

    # All ownerships combined. 2003-2008 national industry files have no
    # ownership split; verified against May 2009, where the by-ownership rows
    # sum to the plain rows (median ratio 1.000).
    ALL_OWNERSHIP_CODE = "1235"

    # US territories, by FIPS — area_type 3 rather than 2.
    TERRITORY_FIPS = {"60", "66", "69", "72", "78"}

    # Source sentinels (from the release's own Field Descriptions sheet).
    #   *  wage estimate not available
    #   ** employment estimate not available
    #   #  wage >= $115.00/hour or $239,200/year (top-coded, not missing)
    #   ~  percent of establishments reporting < 0.5% (bound, not missing)
    SENTINELS = ["*", "**", "#", "~", "-", ""]
    TOP_CODE = "#"
    BELOW_THRESHOLD = "~"

    # Source header (lowercased, spaces -> underscores) -> architecture name.
    # Covers every one of the 14 distinct header shapes across 2003-2025.
    RENAME = {
        "area": "area_id",
        "area_title": "area_name",
        "area_name": "area_name",
        "state": "area_name",
        "area_type": "area_type",
        "prim_state": "state_abbreviation",
        "st": "state_abbreviation",
        "naics": "industry_id",
        "naics_title": "industry_name",
        "i_group": "industry_group",
        "own_code": "ownership_id",
        "ownership": "_ownership_title",
        "occ_code": "occupation_id",
        "occ_title": "occupation_name",
        "o_group": "occupation_group",
        "group": "occupation_group",
        "tot_emp": "employment",
        "emp_prse": "employment_prse",
        "jobs_1000": "jobs_per_1000",
        "loc_quotient": "location_quotient",
        "loc_q": "location_quotient",
        "pct_total": "percent_total_employment",
        "pct_tot": "percent_total_employment",
        "pct_rpt": "percent_establishments_reporting",
        "h_mean": "hourly_wage_mean",
        "a_mean": "annual_wage_mean",
        "mean_prse": "wage_mean_prse",
        "h_pct10": "hourly_wage_percentile_10",
        "h_pct25": "hourly_wage_percentile_25",
        "h_median": "hourly_wage_median",
        "h_pct75": "hourly_wage_percentile_75",
        "h_pct90": "hourly_wage_percentile_90",
        "a_pct10": "annual_wage_percentile_10",
        "a_pct25": "annual_wage_percentile_25",
        "a_median": "annual_wage_median",
        "a_pct75": "annual_wage_percentile_75",
        "a_pct90": "annual_wage_percentile_90",
        "annual": "annual_wage_only",
        "hourly": "hourly_wage_only",
    }

    HOURLY_WAGE_COLUMNS = [
        "hourly_wage_mean",
        "hourly_wage_percentile_10",
        "hourly_wage_percentile_25",
        "hourly_wage_median",
        "hourly_wage_percentile_75",
        "hourly_wage_percentile_90",
    ]
    ANNUAL_WAGE_COLUMNS = [
        "annual_wage_mean",
        "annual_wage_percentile_10",
        "annual_wage_percentile_25",
        "annual_wage_median",
        "annual_wage_percentile_75",
        "annual_wage_percentile_90",
    ]

    # Table keys, used by the uniqueness assertions and the dbt tests. BLS
    # republishes some rows under two different level tags with identical
    # values, so the level tags are part of the key.
    KEYS = {
        "area": [
            "year",
            "area_id",
            "ownership_id",
            "occupation_id",
            "occupation_group",
        ],
        "industry": [
            "year",
            "industry_id",
            "ownership_id",
            "occupation_id",
            "occupation_group",
            "industry_group",
        ],
    }

    # code -> label for the columns whose stored values are opaque codes. The
    # level tags (occupation_group, industry_group) are deliberately absent:
    # they are stored as readable labels already ("detailed", "cross-industry"),
    # so a dictionary entry would restate the value.
    #
    # Labels are quoted from the release's own Field Descriptions sheet. Area
    # type 5 no longer appears from 2019 but is published for 2005-2018.
    DICTIONARY_LABELS = {
        "area_type": {
            "1": "U.S.",
            "2": "State",
            "3": "U.S. Territory",
            "4": "Metropolitan Statistical Area (MSA)",
            "5": "Metropolitan Division",
            "6": "Nonmetropolitan Area",
        },
        "ownership_id": {
            "1": "Federal Government",
            "2": "State Government",
            "3": "Local Government",
            "5": "Private",
            "35": "Private and Local Government",
            "57": (
                "Private, Local Government Gambling Establishments (Sector 71), "
                "and Local Government Casino Hotels (Sector 72)"
            ),
            "58": "Private plus State and Local Government Hospitals",
            "59": "Private and Postal Service",
            "123": "Federal, State, and Local Government",
            "235": "Private, State, and Local Government",
            "1235": "Federal, State, and Local Government and Private Sector",
        },
    }

    ARCHITECTURE_DIR = _CODE_DIR / "architecture"
    # AREA -> AREA_TYPE lookup pooled from the 2011-2013 releases, used to
    # recover area_type for the 2005-2010 metropolitan files, which do not
    # carry it. Generated by models/us_bls_oes/code/build_area_type_map.py.
    AREA_TYPE_MAP = _CODE_DIR / "area_type_map.csv"
