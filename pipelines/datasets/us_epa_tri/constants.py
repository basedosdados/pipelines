"""Constants for the us_epa_tri (EPA Toxics Release Inventory) pipeline."""

from enum import Enum
from pathlib import Path

# repo root: pipelines/datasets/us_epa_tri/constants.py -> up 3
REPO_ROOT = Path(__file__).resolve().parents[3]


class constants(Enum):
    DATASET_ID = "us_epa_tri"

    # The Basic Data Files page. Its "Update Status" box carries the date the
    # files were last regenerated ("Includes reporting forms processed as of:
    # <Month D, YYYY>") and the year dropdown lists every reporting year
    # available. Both are the pipeline's poll signal.
    PAGE_URL = (
        "https://www.epa.gov/toxics-release-inventory-tri-program/"
        "tri-basic-data-files-calendar-years-1987-present"
    )
    # National (all states + DC + territories) Basic Data File for one
    # reporting year, generated on the fly by Envirofacts. ~60 MB of CSV per
    # year, served at ~150 KB/s per connection, so a year takes 5-10 minutes.
    DOWNLOAD_URL = (
        "https://data.epa.gov/efservice/downloads/tri/mv_tri_basic_download/"
        "{year}_US/csv"
    )
    # Envirofacts TRI_FACILITY table: the only place the county FIPS code of a
    # TRI facility is published (the Basic file carries the county name only).
    FACILITY_URL = (
        "https://data.epa.gov/efservice/tri_facility/rows/{start}:{end}/JSON"
    )
    FACILITY_PAGE = 10_000

    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"
        ),
    }

    FIRST_YEAR = 1987
    # Number of columns in a Basic Data File (August 2024 layout).
    N_COLUMNS = 122

    ARCHITECTURE_DIR = (
        REPO_ROOT / "models" / "us_epa_tri" / "code" / "architecture"
    )

    TABLES = ["facility", "chemical", "form", "release", "dicionario"]
    # Tables partitioned by reporting year (chemical and dicionario are not).
    YEAR_TABLES = ["facility", "form", "release"]

    # Grams per pound: dioxin quantities are reported in grams.
    GRAMS_PER_POUND = 453.59237

    # NAICS vintage in force for the codes of each reporting year, measured
    # against the br_bd_diretorios_us vintage directories (RY 2012 codes match
    # NAICS 2007 at 98.7% and NAICS 2012 at 88%: TRI adopted each revision the
    # year after Census). EPA assigned the 1987-2005 codes (Appendix D of the
    # documentation) in the RY 2006 vintage, i.e. NAICS 2002.
    # Source sentinels mapped to NULL: SIC "INVA" (invalid) and "NA"; county
    # FIPS "00000" (unknown).
    SIC_SENTINELS = ["INVA", "NA"]
    UNKNOWN_COUNTY = "00000"
    NAICS_VERSION = [
        (1987, 2007, "2002"),
        (2008, 2012, "2007"),
        (2013, 2016, "2012"),
        (2017, 2021, "2017"),
        (2022, 9999, "2022"),
    ]
