"""Constants for au_abs_population (ABS population estimates and projections).

Three ABS products feed this dataset:

* **3101.0** National, state and territory population — quarterly time-series
  workbooks (ERP and components of change) plus annual ERP by single year of
  age and sex.
* **3218.0** Regional population — annual data cubes by SA2 and LGA.
* **3222.0** Population projections — time-series workbooks, three series
  (A high / B medium / C low), by state, sex and single year of age.

3101.0 and 3222.0 share the standard ABS series-ID workbook layout, so one
parser handles both. The 3218.0 data cubes are a wider, hand-laid-out format
and are parsed separately into the same long shape.
"""

from enum import Enum


class constants(Enum):
    DATASET_ID = "au_abs_population"

    # www.abs.gov.au rejects the default requests User-Agent.
    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"
        )
    }

    BASE = "https://www.abs.gov.au/statistics/people/population"

    # Landing pages carry the current release slug in their download links.
    # 3101.0 is dated by reference quarter ("dec-2025"); 3218.0 by reference
    # financial year ("2024-25"); 3222.0 by projection base ("2022-base-2071").
    LANDING = {
        "national_state": f"{BASE}/national-state-and-territory-population/latest-release",
        "regional": f"{BASE}/regional-population/latest-release",
        "projection": f"{BASE}/population-projections-australia/latest-release",
    }
    PATH = {
        "national_state": f"{BASE}/national-state-and-territory-population/{{slug}}/{{file}}.xlsx",
        "regional": f"{BASE}/regional-population/{{slug}}/{{file}}.xlsx",
        "projection": f"{BASE}/population-projections-australia/{{slug}}/{{file}}.xlsx",
    }
    # Regex that recognises each product's release slug on its landing page.
    SLUG_RE = {
        "national_state": r"national-state-and-territory-population/([a-z]{3}-\d{4})/",
        "regional": r"regional-population/(\d{4}-\d{2})/",
        "projection": r"population-projections-australia/(\d{4}-base-\d{4})/",
    }

    # ---------------- 3101.0 ----------------
    # Quarterly workbooks. Every series is (measure, region) except 310104,
    # which is (measure, sex, region).
    NST_QUARTERLY = ["310101", "310102", "3101016A", "3101016B", "310104"]

    # Annual ERP by single year of age and sex: one workbook per region. The
    # region is carried by the workbook, not by the series description.
    NST_AGE_SEX = {
        "3101051": "1",  # New South Wales
        "3101052": "2",  # Victoria
        "3101053": "3",  # Queensland
        "3101054": "4",  # South Australia
        "3101055": "5",  # Western Australia
        "3101056": "6",  # Tasmania
        "3101057": "7",  # Northern Territory
        "3101058": "8",  # Australian Capital Territory
        "3101059": None,  # Australia (national aggregate, not a state)
    }

    # ---------------- 3222.0 ----------------
    # Table letter -> projection series. ABS labels them "Series 1(A)",
    # "Series 29(B)" and "Series 45(C)"; the published names are high/medium/low.
    PROJ_SERIES = {"A": "high", "B": "medium", "C": "low"}
    # Table number -> state id, mirroring NST_AGE_SEX (9 = Australia).
    PROJ_REGION = {
        "1": "1",
        "2": "2",
        "3": "3",
        "4": "4",
        "5": "5",
        "6": "6",
        "7": "7",
        "8": "8",
        "9": None,
    }

    # ---------------- 3218.0 ----------------
    # cube -> {sheet: what it holds}. Only the finest grain of each geography
    # is ingested: every coarser ASGS level published by ABS (SA3, SA4, GCCSA,
    # state) was verified to be the exact sum of its SA2s, so it is recomputable
    # rather than stored. See models/au_abs_population/ONBOARDING_PLAN.md.
    REGIONAL_CUBES = {
        "erp_sa2": ("32180DS0003_{span}", "Table 1", "SA2 code"),
        "erp_lga": ("32180DS0004_{span}", "Table 1", "LGA code"),
        "components_sa2": ("32180DS0005_{span}", "Table 1", "SA2 code"),
        "components_lga": ("32180DS0006_{span}", "Table 1", "LGA code"),
        "area_sa2": ("32180DS0001_{span}", None, "SA2 code"),
        "area_lga": ("32180DS0002_{span}", None, "LGA code"),
    }

    # Component measures, in the order ABS lays them out in DS0005/DS0006.
    COMPONENTS = [
        "births",
        "deaths",
        "natural_increase",
        "internal_arrivals",
        "internal_departures",
        "net_internal_migration",
        "overseas_arrivals",
        "overseas_departures",
        "net_overseas_migration",
    ]

    # Values ABS uses for "not applicable" / "not available" in the data cubes.
    NULL_SENTINELS = {"..", "...", "-", "na", "n.a.", "np", "np.", ""}

    # ABS region labels -> br_bd_diretorios_au.state:id_state. Both the full
    # names used by 3101.0 and the abbreviations used by 3222.0 appear.
    STATE_ID = {
        "new south wales": "1",
        "nsw": "1",
        "victoria": "2",
        "vic": "2",
        "queensland": "3",
        "qld": "3",
        "south australia": "4",
        "sa": "4",
        "western australia": "5",
        "wa": "5",
        "tasmania": "6",
        "tas": "6",
        "northern territory": "7",
        "nt": "7",
        "australian capital territory": "8",
        "act": "8",
        "other territories": "9",
        "ot": "9",
    }

    # Column order per output table; matches the architecture CSVs exactly.
    COLUMNS = {
        "national_state": [
            "year",
            "quarter",
            "geography_level",
            "state_id",
            "region_name",
            "sex",
            "measure",
            "unit",
            "series_id",
            "value",
        ],
        "erp_age_sex": [
            "year",
            "geography_level",
            "state_id",
            "region_name",
            "sex",
            "age",
            "series_id",
            "erp",
        ],
        "projection": [
            "year",
            "projection_base_year",
            "series",
            "geography_level",
            "state_id",
            "region_name",
            "sex",
            "age",
            "series_id",
            "projected_population",
        ],
        "regional_sa2": [
            "year",
            "sa2_id",
            "sa2_name",
            "sa3_id",
            "sa4_id",
            "gccsa_id",
            "state_id",
            "erp",
            "births",
            "deaths",
            "natural_increase",
            "internal_arrivals",
            "internal_departures",
            "net_internal_migration",
            "overseas_arrivals",
            "overseas_departures",
            "net_overseas_migration",
            "area_sqkm",
            "population_density",
        ],
        "regional_lga": [
            "year",
            "lga_id",
            "lga_name",
            "state_id",
            "erp",
            "births",
            "deaths",
            "natural_increase",
            "internal_arrivals",
            "internal_departures",
            "net_internal_migration",
            "overseas_arrivals",
            "overseas_departures",
            "net_overseas_migration",
            "area_sqkm",
            "population_density",
        ],
        "series": [
            "series_id",
            "description",
            "unit",
            "frequency",
            "source_catalogue",
            "source_table",
            "series_start",
            "series_end",
        ],
    }

    # Tables that are partitioned by year in staging and in BigQuery.
    PARTITIONED = [
        "national_state",
        "erp_age_sex",
        "projection",
        "regional_sa2",
        "regional_lga",
    ]
