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

    # OpenAustralia mirror, used by the recurring pipeline. ParlInfo answers the
    # Prefect worker with HTTP 403 on every request - verified 2026-09-02, 490 of
    # 490 probes - while serving the same code and headers from an Australian
    # connection, so the block is on the worker's egress IP. OpenAustralia
    # publishes a parsed mirror of the same Hansard, built for bulk access.
    OPENAUSTRALIA_BASE = "https://data.openaustralia.org.au/scrapedxml"
    OPENAUSTRALIA_DIRS = {
        "hofreps": "representatives_debates",
        "senate": "senate_debates",
    }
    # Speaker rosters, which carry the electorate and party the debate XML omits.
    OPENAUSTRALIA_ROSTER = (
        "https://raw.githubusercontent.com/openaustralia/"
        "openaustralia-parser/master/data"
    )
    # OpenAustralia inherits the UK codebase's namespaces: representatives are
    # `member/<n>` and senators `lord/<100000 + n>`, both indexing the same
    # "member count" column in the rosters.
    OPENAUSTRALIA_SENATOR_ID_OFFSET = 100000
    # Earliest sitting day the mirror carries.
    OPENAUSTRALIA_FIRST_YEAR = 2006

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
