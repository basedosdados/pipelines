"""Constants for the us_irs_form990 pipeline (IRS Form 990 series)."""

from enum import Enum
from pathlib import Path

# repo root: pipelines/datasets/us_irs_form990/constants.py -> up 4
REPO_ROOT = Path(__file__).resolve().parents[3]


class constants(Enum):
    DATASET_ID = "us_irs_form990"

    # irs.gov and apps.irs.gov answer scripted clients, but a browser user
    # agent is kept for parity with the other US federal sources.
    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"
        ),
    }

    # --- e-file XML (Form 990 series downloads) ---------------------------
    # The listing page names every ZIP the IRS currently serves. The ZIPs are
    # keyed by *release* year (the year the IRS posted them), not tax year;
    # each holds several tax years of returns.
    EFILE_LISTING_URL = (
        "https://www.irs.gov/charities-non-profits/form-990-series-downloads"
    )
    EFILE_BASE_URL = "https://apps.irs.gov/pub/epostcard/990/xml/"
    # Release years 2017 and 2018 are still hosted at the same path but are
    # no longer linked from the listing page; they are enumerated by pattern.
    EFILE_UNLISTED_YEARS = {2017: 7, 2018: 7}  # year -> number of parts

    # Form types the concordance maps. 990-PF (private foundations) and 990-T
    # are not covered by the concordance and are skipped by the transform.
    RETURN_TYPES = {"990", "990EZ"}

    # --- Exempt Organizations Business Master File (monthly registry) -----
    BMF_BASE_URL = "https://www.irs.gov/pub/irs-soi/"
    # Four regional files plus Puerto Rico and International, which together
    # cover every record on the file.
    BMF_FILES = ["eo1", "eo2", "eo3", "eo4", "eo_pr", "eo_xx"]
    BMF_INFO_URL = "https://www.irs.gov/pub/foia/ig/tege/eo-info.pdf"

    # --- Automatic revocation list (monthly, full replacement) -------------
    REVOCATION_URL = (
        "https://apps.irs.gov/pub/epostcard/data-download-revocation.zip"
    )
    REVOCATION_COLUMNS = [
        "ein",
        "legal_name",
        "doing_business_as_name",
        "address",
        "city",
        "state",
        "zip_code",
        "country",
        "exemption_type",
        "revocation_date",
        "revocation_posting_date",
        "exemption_reinstatement_date",
    ]

    ARCHITECTURE_DIR = (
        REPO_ROOT / "models" / "us_irs_form990" / "code" / "architecture"
    )
    # Trimmed copy of the NODC master concordance (MIT): only the variables
    # the transform reads. Built by models/us_irs_form990/code/build_concordance.py.
    CONCORDANCE_PATH = Path(__file__).resolve().parent / "concordance.csv"

    TABLES = [
        "organization",
        "return_financial",
        "compensation",
        "revocation",
        "dicionario",
    ]

    # Rows per output parquet file.
    CHUNK_ROWS = 500_000
