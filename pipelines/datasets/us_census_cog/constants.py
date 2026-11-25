"""Constants for the us_census_cog dataset (U.S. Census of Governments).

The Census of Governments is published as three loosely-coupled file families,
each with its own directory tree, packaging and naming on ``www2.census.gov``:

* **GUS** (Government Units Survey) -- one Excel workbook per survey year, one
  worksheet per type of government.
* **APES** (Annual Survey of Public Employment & Payroll, the census-year
  edition is branded COG-E) -- a fixed-width data file plus a fixed-width unit
  directory, per year, packaged half a dozen different ways over 1992-2024.
* **Finance** -- a single 262 MB archive holding a wide comma-delimited file per
  year for 1967-2012, plus one long fixed-width file per year for 2013-2018.

Naming is not derivable from the year alone, so the per-year source files are
enumerated rather than formatted. ``utils.resolve_source`` walks the candidate
list for a year and takes the first URL that answers.
"""

from enum import Enum
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]

WWW2 = "https://www2.census.gov/programs-surveys"

# Government Units Survey: one workbook per year. 2002 and 2007 are published
# in a different, per-type ``gid-*.zip`` layout and are deliberately excluded --
# see models/us_census_cog/CLAUDE.md.
GUS_YEARS = [1997, 2012, 2017, 2021, 2022, 2024, 2025]
GUS_FILES = {
    1997: "govt_units_1997.zip",
    2012: "govt_units_2012.zip",
    2017: "govt_units_2017.ZIP",
    2021: "govt_units_2021.ZIP",
    2022: "govt_units_2022.ZIP",
    2024: "gov_units_2024.zip",
    2025: "gov_units_2025.zip",
}

# 1996 is absent: the Census Bureau ran no employment survey that year, and
# www2.census.gov/programs-surveys/apes/datasets/1996/ is empty.
EMPLOYMENT_YEARS = [y for y in range(1992, 2025) if y != 1996]
# Years whose data and unit-directory files ship inside one bundle rather than
# as a separate ``<yy>empst`` / ``<yy>empid`` pair.
EMPLOYMENT_BUNDLES = {
    2012: "2012/annual-apes/2012_downloadable_data.zip",
    2013: "2013/annual-apes/2013_downloadable_data.zip",
    2014: "2014/annual-apes/2014_individual_unit_files.zip",
    2015: "2015/annual-apes/2015_individual_unit_files.zip",
    2016: "2016/annual-apes/2016_downloadable_data.zip",
    2017: "2017/2017_individual_unit_files.zip",
    2018: "2018/2018_individual_unit_files.zip",
    2019: "2019/2019_individual_unit_files.zip",
    2020: "2020/2020_individual_unit_files.zip",
    2021: "2021/2021_individual_unit_files.zip",
    2022: "2022/2022%20COG-E%20Individual%20Unit%20Files.zip",
    2023: "2023/2023_individual_unit_files.zip",
    2024: "2024/2024_individual_unit_files.zip",
}
# 1992-2011 ship a zipped pair; census years prefix the stem with "c".
EMPLOYMENT_CENSUS_YEARS = {1992, 1997, 2002, 2007}

FINANCE_HISTORICAL = (
    f"{WWW2}/gov-finances/datasets/historical/_IndFin_1967-2012.zip"
)
# Fiscal years present in the historical archive. 1968 and 1969 were never
# collected.
FINANCE_HISTORICAL_YEARS = [1967, *range(1970, 2013)]
FINANCE_MODERN_FILES = {
    2013: "2013/public-use-datasets/2013-individual-unit-file-revised.zip",
    2014: "2014/public-use-datasets/2014-individual-unit-file.zip",
    2015: "2015/public-use-datasets/2015-individual-unit-file.zip",
    2016: "2016/public-use-datasets/2016_Individual_Unit_file.zip",
    2017: "2017/public-use-datasets/2017_Individual_Unit_File.zip",
    2018: "2018/2018_Individual_Unit_File.zip",
}
FINANCE_YEARS = [*FINANCE_HISTORICAL_YEARS, *sorted(FINANCE_MODERN_FILES)]


class constants(Enum):
    """Enum of dataset constants."""

    DATASET_ID = "us_census_cog"
    ALL_TABLES = [
        "government_unit",
        "employment",
        "employment_unit",
        "finance",
        "finance_unit",
        "dicionario",
    ]
    DATA_TABLES = [
        "government_unit",
        "employment",
        "employment_unit",
        "finance",
        "finance_unit",
    ]
    ARCHITECTURE_DIR = str(
        REPO_ROOT / "models" / "us_census_cog" / "code" / "architecture"
    )
    # www2.census.gov fronts a firewall that answers HTTP 200 with a
    # "Request Rejected" HTML body for a small, arbitrary set of valid URLs.
    # The rejection is cached against the exact URL, so a cache-busting query
    # string recovers the real file.
    HTTP_RETRIES = 5
    USER_AGENT = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36"
    )


# The employment files identify states by a 2-digit GOVS code -- the states in
# alphabetical order, 00 = United States -- while the finance files and every
# BD directory use FIPS. California is 05 in one scheme and 06 in the other, so
# the two are not interchangeable and the map below is load-bearing.
GOVS_STATE_ABBREVIATION = {
    "00": "US",
    "01": "AL",
    "02": "AK",
    "03": "AZ",
    "04": "AR",
    "05": "CA",
    "06": "CO",
    "07": "CT",
    "08": "DE",
    "09": "DC",
    "10": "FL",
    "11": "GA",
    "12": "HI",
    "13": "ID",
    "14": "IL",
    "15": "IN",
    "16": "IA",
    "17": "KS",
    "18": "KY",
    "19": "LA",
    "20": "ME",
    "21": "MD",
    "22": "MA",
    "23": "MI",
    "24": "MN",
    "25": "MS",
    "26": "MO",
    "27": "MT",
    "28": "NE",
    "29": "NV",
    "30": "NH",
    "31": "NJ",
    "32": "NM",
    "33": "NY",
    "34": "NC",
    "35": "ND",
    "36": "OH",
    "37": "OK",
    "38": "OR",
    "39": "PA",
    "40": "RI",
    "41": "SC",
    "42": "SD",
    "43": "TN",
    "44": "TX",
    "45": "UT",
    "46": "VT",
    "47": "VA",
    "48": "WA",
    "49": "WV",
    "50": "WI",
    "51": "WY",
}
FIPS_STATE = {
    "AL": "01",
    "AK": "02",
    "AZ": "04",
    "AR": "05",
    "CA": "06",
    "CO": "08",
    "CT": "09",
    "DE": "10",
    "DC": "11",
    "FL": "12",
    "GA": "13",
    "HI": "15",
    "ID": "16",
    "IL": "17",
    "IN": "18",
    "IA": "19",
    "KS": "20",
    "KY": "21",
    "LA": "22",
    "ME": "23",
    "MD": "24",
    "MA": "25",
    "MI": "26",
    "MN": "27",
    "MS": "28",
    "MO": "29",
    "MT": "30",
    "NE": "31",
    "NV": "32",
    "NH": "33",
    "NJ": "34",
    "NM": "35",
    "NY": "36",
    "NC": "37",
    "ND": "38",
    "OH": "39",
    "OK": "40",
    "OR": "41",
    "PA": "42",
    "RI": "44",
    "SC": "45",
    "SD": "46",
    "TN": "47",
    "TX": "48",
    "UT": "49",
    "VT": "50",
    "VA": "51",
    "WA": "53",
    "WV": "54",
    "WI": "55",
    "WY": "56",
}
GOVS_TO_FIPS_STATE = {
    govs: FIPS_STATE[ab]
    for govs, ab in GOVS_STATE_ABBREVIATION.items()
    if ab in FIPS_STATE
}

# Fixed-width field maps for the employment individual-unit data file, keyed by
# record length. Every era shares the leading identifier and function code; the
# eras differ in whether each measure carries a data flag, and in whether
# part-time hours, full-time-equivalent employment and the 6-digit unit id are
# present at all.
EMPLOYMENT_DATA_LAYOUTS = {
    # 1992-2006: no data flags, and no 6-digit id.
    84: {
        "unit_id_govs": (1, 14),
        "function_code": (18, 20),
        "full_time_employees": (21, 30),
        "full_time_payroll": (31, 42),
        "part_time_employees": (43, 52),
        "part_time_payroll": (53, 64),
        "part_time_hours": (65, 74),
        "full_time_equivalent_employees": (75, 84),
    },
    # 2007-2011 (96) and 2012-2018 (94) are the same layout; the 96-character
    # records simply carry two trailing spaces.
    96: {
        "unit_id_govs": (1, 14),
        "function_code": (18, 20),
        "full_time_employees": (21, 30),
        "full_time_employees_flag": (32, 32),
        "full_time_payroll": (33, 44),
        "full_time_payroll_flag": (46, 46),
        "part_time_employees": (47, 56),
        "part_time_employees_flag": (58, 58),
        "part_time_payroll": (59, 70),
        "part_time_payroll_flag": (72, 72),
        "part_time_hours": (73, 82),
        "part_time_hours_flag": (84, 84),
        "full_time_equivalent_employees": (85, 94),
    },
    # 2019-2020: part-time hours and full-time-equivalent employment dropped.
    72: {
        "unit_id_govs": (1, 14),
        "function_code": (18, 20),
        "full_time_employees": (21, 30),
        "full_time_employees_flag": (32, 32),
        "full_time_payroll": (33, 44),
        "full_time_payroll_flag": (46, 46),
        "part_time_employees": (47, 56),
        "part_time_employees_flag": (58, 58),
        "part_time_payroll": (59, 70),
        "part_time_payroll_flag": (72, 72),
    },
    # 2021-2024: adds the 6-digit government id that replaces the legacy one.
    80: {
        "unit_id_govs": (1, 14),
        "function_code": (18, 20),
        "full_time_employees": (21, 30),
        "full_time_employees_flag": (32, 32),
        "full_time_payroll": (33, 44),
        "full_time_payroll_flag": (46, 46),
        "part_time_employees": (47, 56),
        "part_time_employees_flag": (58, 58),
        "part_time_payroll": (59, 70),
        "part_time_payroll_flag": (72, 72),
        "government_id": (75, 80),
    },
}
EMPLOYMENT_DATA_LAYOUTS[94] = EMPLOYMENT_DATA_LAYOUTS[96]

# The unit-directory file is stable at 206 characters for 1992-2020; 2021 adds
# the 6-digit government id in positions 208-213.
EMPLOYMENT_UNIT_LAYOUT = {
    "unit_id_govs": (1, 14),
    "state_code_govs": (1, 2),
    "government_type": (3, 3),
    "county_code_govs": (4, 6),
    "unit_name": (15, 78),
    "census_region_code": (79, 79),
    "county_name": (80, 109),
    "state_id": (110, 111),
    "county_code": (112, 114),
    "population_enrollment_function": (126, 134),
    "population_enrollment_year": (135, 136),
    "school_level_code": (137, 138),
    "selection_probability": (146, 151),
    "worksheet_code": (205, 206),
}
EMPLOYMENT_UNIT_LAYOUT_PID6 = dict(
    EMPLOYMENT_UNIT_LAYOUT, government_id=(208, 213)
)

# Finance individual-unit files, 2013-2018, keyed by record length. Fiscal 2017
# is the only year published against the 6-digit government id; every other year
# in the range uses the same 14-character GOVS identifier as the historical
# archive and the employment files, so the two shapes must be told apart by
# record length rather than by year.
FINANCE_DATA_LAYOUTS = {
    # 2013-2016 and 2018: GOVS identifier.
    34: {
        "government_id_govs": (1, 14),
        "state_code_govs": (1, 2),
        "government_type": (3, 3),
        "item_code": (15, 17),
        "amount": (18, 29),
        "year": (30, 33),
        "data_flag": (34, 34),
    },
    # 2017: FIPS state and county, then the 6-digit government id.
    32: {
        "state_id": (1, 2),
        "government_type": (3, 3),
        "county_code": (4, 6),
        "government_id": (7, 12),
        "item_code": (13, 15),
        "amount": (16, 27),
        "year": (28, 31),
        "data_flag": (32, 32),
    },
}
# Finance unit-directory files, keyed by record length.
FINANCE_UNIT_LAYOUTS = {
    # Fin_GID_<year>.txt, 2013-2016 and 2018.
    153: {
        "government_id_govs": (1, 14),
        "state_code_govs": (1, 2),
        "government_type": (3, 3),
        "unit_name": (15, 78),
        "county_name": (79, 113),
        "state_id": (114, 115),
        "county_code": (116, 118),
        "place_code": (119, 123),
        "population": (124, 132),
        "population_year": (133, 134),
        "school_enrollment": (135, 141),
        "enrollment_year": (142, 143),
        "special_district_function_code": (144, 145),
        "school_level_code": (146, 147),
        "fiscal_year_end": (148, 151),
    },
    # Fin_PID_2017.txt.
    146: {
        "state_id": (1, 2),
        "government_type": (3, 3),
        "county_code": (4, 6),
        "government_id": (7, 12),
        "unit_name": (13, 76),
        "county_name": (77, 111),
        "place_code": (112, 116),
        "population": (117, 125),
        "population_year": (126, 127),
        "school_enrollment": (128, 134),
        "enrollment_year": (135, 136),
        "special_district_function_code": (137, 138),
        "school_level_code": (139, 140),
        "fiscal_year_end": (141, 144),
    },
}
