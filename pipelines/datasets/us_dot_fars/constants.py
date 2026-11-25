"""Constants for the us_dot_fars pipeline.

FARS (Fatality Analysis Reporting System) is NHTSA's annual census of fatal
motor-vehicle crashes on US public roads. NHTSA republishes every year from 1975
onward as a single CSV zip, so one download shape covers the whole 50-year span.
"""

from enum import Enum
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[3]


class constants(Enum):
    """Constant values for the us_dot_fars pipeline."""

    DATASET_ID = "us_dot_fars"

    # The architecture CSVs are the single source of truth for column names,
    # order, types and the raw -> clean name mapping. The transform, the dbt
    # models and the backend metadata are all generated from them.
    ARCHITECTURE_DIR = str(
        _REPO_ROOT / "models" / "us_dot_fars" / "code" / "architecture"
    )

    DATA_TABLES = ["crash", "vehicle", "person"]
    ALL_TABLES = ["crash", "vehicle", "person", "dicionario"]

    # The table the poll, the source Update and the flow-run name hang off.
    # One raw data source may be linked per table: client._raw_source_id resolves
    # a table's source through a query that raises when a table has two or more.
    PRIMARY_TABLE = "crash"

    # Source file (inside the annual zip) backing each table.
    SOURCE_FILE = {
        "crash": "accident",
        "vehicle": "vehicle",
        "person": "person",
    }

    FIRST_YEAR = 1975

    DOWNLOAD_URL = (
        "https://static.nhtsa.gov/nhtsa/downloads/FARS/"
        "{year}/National/FARS{year}NationalCSV.zip"
    )
    # The SAS release of the same year carries NHTSA's own PROC FORMAT source,
    # which is the authoritative code -> label map for 1975-2014. From 2015 the
    # CSVs ship <VAR>NAME companion columns and the SAS download is not needed.
    FORMAT_URL = (
        "https://static.nhtsa.gov/nhtsa/downloads/FARS/"
        "{year}/National/FARS{year}NationalSAS.zip"
    )
    LAST_FORMAT_YEAR = 2014

    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
        )
    }

    # Codes that mean "not a real county": 0 is "not applicable" and 997-999 are
    # the unknown/not-reported band. None of them may become a county FIPS.
    COUNTY_SENTINELS = {0, 997, 998, 999}
