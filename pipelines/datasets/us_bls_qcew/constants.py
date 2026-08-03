"""Constants for us_bls_qcew (BLS Quarterly Census of Employment and Wages).

Shared by the one-shot onboarding bootstrap (``models/us_bls_qcew/code/``) and,
later, the recurring Prefect pipeline. Holds source URLs, the browser
User-Agent that ``data.bls.gov`` requires, the coverage year ranges, and the
``agglvl_code`` → geographic-level routing (which differs between NAICS and SIC).
"""

from enum import Enum
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[3]


class constants(Enum):
    """Static configuration for the QCEW dataset."""

    DATASET_ID = "us_bls_qcew"

    # data.bls.gov returns 403 without a browser-like User-Agent.
    USER_AGENT = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 "
        "(Data Basis dataset onboarding; contact rdahis@basedosdados.org)"
    )
    REFERER = "https://www.bls.gov/cew/downloadable-data-files.htm"

    # Per-year singlefile zip URL templates.
    NAICS_URL = "https://data.bls.gov/cew/data/files/{year}/csv/{year}_{freq}_singlefile.zip"
    SIC_URL = "https://data.bls.gov/cew/data/files/{year}/sic/csv/sic_{year}_{freq}_singlefile.zip"
    # Title CSVs (for the dicionario). {p}="" for NAICS, "sic_" for SIC.
    TITLE_URL = (
        "https://data.bls.gov/cew/doc/titles/{kind}/{p}{kind}_titles.csv"
    )

    # BLS names the frequency segment differently in the two URL families.
    FREQ_SEGMENT = {"quarterly": "qtrly", "annual": "annual"}

    # Coverage.
    NAICS_YEARS = list(range(1990, 2026))  # 1990-2025
    SIC_YEARS = list(range(1975, 2001))  # 1975-2000

    CLASSES = ["naics", "sic"]
    FREQS = ["quarterly", "annual"]
    GEOS = ["national", "state", "county", "metro"]

    # Representative subset used for the dev verification checkpoint; the full
    # backfill runs after the user approves at that checkpoint.
    SUBSET_YEARS = {
        "naics": [1990, 2024],
        "sic": [1975, 2000],
    }

    # agglvl_code (2-char) → geographic level. The two classifications use
    # entirely different aggregation-level schemes.
    NAICS_AGGLVL_GEO = {
        # national: 10-18 (by industry), 21-28 (by size), 91-95 (U.S.-wide specials)
        **{f"{c}": "national" for c in range(10, 19)},
        **{f"{c}": "national" for c in range(21, 29)},
        **{f"{c}": "national" for c in range(91, 96)},
        # 96 ("Total Government, by State") carries per-state area_fips -> state
        "96": "state",
        # metro: 30 (CSA), 40-48 (MSA), 80 (MicroSA)
        "30": "metro",
        **{f"{c}": "metro" for c in range(40, 49)},
        "80": "metro",
        # state: 50-58 (by industry), 61-64 (by size)
        **{f"{c}": "state" for c in range(50, 59)},
        **{f"{c}": "state" for c in range(61, 65)},
        # county: 70-78
        **{f"{c}": "county" for c in range(70, 79)},
    }
    SIC_AGGLVL_GEO = {
        # national 01-11, metro (MSA) 12-17, state 18-25, county 26-31
        **{f"{c:02d}": "national" for c in range(1, 12)},
        **{f"{c:02d}": "metro" for c in range(12, 18)},
        **{f"{c:02d}": "state" for c in range(18, 26)},
        **{f"{c:02d}": "county" for c in range(26, 32)},
    }

    ARCHITECTURE_DIR = (
        _REPO_ROOT / "models" / "us_bls_qcew" / "code" / "architecture"
    )
