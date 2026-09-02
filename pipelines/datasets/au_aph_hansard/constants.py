"""Constants for the au_aph_hansard dataset (Australian Parliament Hansard)."""

from enum import Enum
from pathlib import Path


class constants(Enum):
    """Constants for au_aph_hansard."""

    DATASET_ID = "au_aph_hansard"

    PARLINFO_HOST = "https://parlinfo.aph.gov.au"

    # Sitting-day index covering 1901-2005, harvested by Tim Sherratt (GLAM Workbench).
    # Saves ~11k ParlInfo search requests. Note it also lists 1981-1997 days whose XML
    # 404s at the source, so every URL must still be test-fetched.
    SITTING_DAY_INDEX_URL = "https://raw.githubusercontent.com/wragge/hansard-xml/master/all-sitting-days.csv"

    # 2016 GLAM Workbench mirror of ParlInfo. ParlInfo itself no longer serves
    # most pre-1998 transcripts (it answers with a "Missing File" page and
    # HTTP 200), so this is the only working source for 1901-1997.
    MIRROR_RAW_URL = (
        "https://raw.githubusercontent.com/wragge/hansard-xml/master"
    )

    # ParlInfo dataset codes. The "80" variants hold the 1901-1980 digitised records.
    CHAMBERS = {
        "hofreps": {
            "modern": "hansardr",
            "historic": "hansardr80",
            "chamber_name": "House of Representatives",
        },
        "senate": {
            "modern": "hansards",
            "historic": "hansards80",
            "chamber_name": "Senate",
        },
    }

    FIRST_YEAR = 1901

    # Index-based enumeration covers this span; later years are probed date by date.
    INDEX_LAST_YEAR = 2005

    HTTP_HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-AU,en;q=0.9",
    }

    ARCHITECTURE_DIR = (
        Path(__file__).resolve().parents[3]
        / "models"
        / "au_aph_hansard"
        / "code"
        / "architecture"
    )

    TABLES = ("speech", "sitting_day")
