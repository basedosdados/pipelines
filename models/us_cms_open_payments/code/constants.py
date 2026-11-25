"""Source catalogue for the CMS Open Payments onboarding.

Two publication regimes coexist:

* Program years 2013-2018 are *archived*. CMS ships one ZIP per program year;
  the ZIP name encodes the publication cycle that last refreshed it.
* Program years 2019-2025 are *current*. CMS ships loose CSVs under a
  per-program-year prefix plus a set of cross-year summary reports, all
  stamped with the current publication cycle.

Two schema eras cut across that split: 2013-2015 (legacy, ``Physician_*``
columns) and 2016 onwards (modern, ``Covered_Recipient_*``). The era boundary
is *not* the archive boundary, so 2016-2018 are archived but modern.
"""

from pathlib import Path

BASE = "https://download.cms.gov/openpayments"

# Publication cycle for the current (non-archived) files.
CURRENT_CYCLE = "P06302026_06032026"

# Program years, and where each one's schema era starts.
LEGACY_YEARS = [2013, 2014, 2015]
MODERN_YEARS = [2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025]
ALL_YEARS = LEGACY_YEARS + MODERN_YEARS

# Program years still served as loose CSVs rather than an archived ZIP.
CURRENT_YEARS = [2019, 2020, 2021, 2022, 2023, 2024, 2025]

# Archived program year -> ZIP file name. Names are not derivable: each year
# was frozen by a different publication cycle, and 2013-2016 predate the
# two-part cycle stamp entirely.
ARCHIVE_ZIPS = {
    2013: "PGYR13_P012221.ZIP",
    2014: "PGYR14_P012122.ZIP",
    2015: "PGYR15_P012023.ZIP",
    2016: "PGYR16_P011824.ZIP",
    2017: "PGYR2017_P01302025_01212025.zip",
    2018: "PGYR2018_P01232026_01102026.zip",
}

# Detail file kind -> the CSV stem CMS uses inside both regimes.
DETAIL_KINDS = {"general": "GNRL", "research": "RSRCH", "ownership": "OWNRSHP"}


def detail_url(year: int, kind: str) -> str:
    """URL of a detail CSV for a *current* program year."""
    stem = DETAIL_KINDS[kind]
    return (
        f"{BASE}/PGYR{year}_{CURRENT_CYCLE}/"
        f"OP_DTL_{stem}_PGYR{year}_{CURRENT_CYCLE}.csv"
    )


def archive_url(year: int) -> str:
    """URL of the archived ZIP holding every detail file for one program year."""
    return f"{BASE}/{ARCHIVE_ZIPS[year]}"


def summary_url(stem: str, joined: bool = False) -> str:
    suffix = "-joined" if joined else ""
    return (
        f"{BASE}/SMRY_RPTS_{CURRENT_CYCLE}/{stem}_{CURRENT_CYCLE}{suffix}.csv"
    )


def profile_url(stem: str) -> str:
    return f"{BASE}/PHPRFL_{CURRENT_CYCLE}/{stem}_{CURRENT_CYCLE}.csv"


# --- Entity / profile files -------------------------------------------------
# One snapshot each, covering every published program year.
PROFILE_FILES = {
    "covered_recipient_profile": profile_url("OP_CVRD_RCPNT_PRFL_SPLMTL"),
    "teaching_hospital_profile": summary_url("PBLCTN_TH_PRFL_SRCH"),
    "reporting_entity_profile": summary_url("PBLCTN_RPTG_ORG_PRFL_SRCH"),
    "provider_profile_mapping": summary_url("PBLCTN_PRVDR_PRFL_MAPPING"),
}

# --- Summary reports --------------------------------------------------------
# Split by whether the file already carries a Program_Year column. The
# per-year family does not, so the program year comes from the file name and
# the PGYRall variants are skipped: they are plain sums over the per-year
# files and would double-count if stacked alongside them.
SUMMARY_PER_YEAR = {
    "summary_by_recipient_nature": "PBLCTN_SMRY_BY_CR_BY_NTR_OF_PYMT_PGYR{year}",
    "summary_by_recipient_entity": "PBLCTN_SMRY_BY_CR_BY_AMGPO_PGYR{year}",
    "summary_by_entity_nature": "PBLCTN_SMRY_BY_AMGPO_BY_NTR_OF_PYMT_PGYR{year}",
    "summary_by_entity_recipient_nature": (
        "PBLCTN_SMRY_BY_AMGPO_BY_CR_BY_NTR_OF_PYMT_PGYR{year}"
    ),
    "summary_state_by_nature": "PBLCTN_NTR_OF_PYMT_BY_STATE_SMRY_PGYR{year}",
}

# stem -> whether CMS appends "-joined" to the file name.
SUMMARY_ALL_YEARS = {
    "summary_national": ("PBLCTN_NTNL_SMRY", True),
    "summary_national_by_specialty": ("PBLCTN_SPLTY_SMRY", True),
    "summary_state": ("PBLCTN_STATE_SMRY", True),
    "summary_teaching_hospital": ("PBLCTN_TH_SMRY", False),
    "summary_reporting_entity": ("PBLCTN_RPTG_ORG_SMRY", False),
    "summary_physician": ("PBLCTN_PHYSN_NON_PHYSN_PRCTNR_SMRY", False),
    "summary_dashboard": ("PBLCTN_DSHBRD", False),
}

# Summary reports are only rebuilt for the current publication, so they cover
# 2019-2025 even though the detail tables reach back to 2013.
SUMMARY_YEARS = CURRENT_YEARS

# --- Local scratch ----------------------------------------------------------
# Never inside the repo or Dropbox: the raw CSVs total roughly 105 GB.
DATA_ROOT = Path.home() / "Downloads" / "us_cms_open_payments_data"
INPUT_DIR = DATA_ROOT / "input"
OUTPUT_DIR = DATA_ROOT / "output"

ARCH_DIR = Path(__file__).resolve().parent / "architecture"

GCP_DATASET_ID = "us_cms_open_payments"
