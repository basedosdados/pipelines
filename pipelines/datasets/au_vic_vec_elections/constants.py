"""Constants for au_vic_vec_elections — Victorian Electoral Commission results."""

import os
from enum import Enum
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
ARCHITECTURE_DIR = (
    REPO_ROOT / "models" / "au_vic_vec_elections" / "code" / "architecture"
)


def data_root() -> Path:
    """Scratch location for raw downloads and cleaned parquet (never inside the repo)."""
    return Path(
        os.environ.get(
            "VEC_DATA_ROOT",
            Path.home() / "Downloads" / "au_vic_vec_elections_data",
        )
    )


class constants(Enum):
    DATASET_ID = "au_vic_vec_elections"
    BACKEND_SLUG = "vic_elections"
    ORGANIZATION_SLUG = "au_vec"

    # The VEC publishes its whole file estate in a publicly listable Azure blob
    # container. Listing it is the authoritative inventory: complete, with sizes and
    # modification times, and it does not depend on scraping the website. The website
    # merely renders these same blobs — the 2018 results page embeds
    # historical-results/state2018/summary.html in an iframe, and each 2022 district
    # page links its State/Reports workbook as a download.
    BLOB_CONTAINER = (
        "https://itsitecoreblobvecprd01.blob.core.windows.net/public-files"
    )

    # The donations register is a Power Apps (Dataverse) portal on a different host,
    # split across two saved views of one entity. Both must be pulled: the "after 2020"
    # grid label is a filter, not the extent of the data.
    DISCLOSURES_BASE_URL = "https://disclosures.vec.vic.gov.au"
    DISCLOSURE_PATHS = ("/public-donations/", "/public-donations-before-2020/")

    # Two-party-preferred for 2022 is not on the blob; it is served from the CMS.
    TWO_PARTY_PREFERRED_2022_URL = "https://www.vec.vic.gov.au/-/media/a8466a1794024583a2128ed431ca24f3.xlsx"

    # Polling day for each electoral event in the dataset.
    ELECTION_DATES = {
        "state2002": "2002-11-30",
        "state2006": "2006-11-25",
        "state2010": "2010-11-27",
        "state2014": "2014-11-29",
        "state2018": "2018-11-24",
        "state2022": "2022-11-26",
        "prahran_by2025": "2025-02-08",
        "nepean_by2026": "2026-05-02",
    }

    # Australian Statistical Geography Standard vintage used for the district link.
    # The ASGS "2021" vintage carries the boundaries of Victoria's 2013 redivision, not
    # the 2021 one: it still lists Keysborough and Buninyong, abolished in 2021, and has
    # no Ashwood, Berwick, Eureka, Glen Waverley, Greenvale, Kalkallo, Laverton,
    # Pakenham or Point Cook, all created in 2021. So it matches the 2014 and 2018
    # districts exactly and only partially matches 2022, 2010 and 2006.
    SED_VINTAGE = "2021"

    # Division ids are re-used across vintages with different meanings: of the 62
    # division names common to the 2011 and 2021 vintages, only 5 keep the same id. Any
    # crosswalk must therefore key on (vintage, name), never on the id alone.
    SED_VINTAGES_AVAILABLE = ("2011", "2016", "2021")

    USER_AGENT = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"
    )

    # Every table in the dataset, in publication order.
    TABLES = [
        "election",
        "candidate",
        "enrolment_turnout",
        "result_district",
        "result_voting_centre",
        "distribution_of_preferences",
        "disclosure_gift",
        "dicionario",
    ]

    PARTITIONED_BY_YEAR = [t for t in TABLES if t != "dicionario"]
