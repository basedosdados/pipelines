"""Constants for the us_fdic_bankfind pipeline."""

from enum import Enum
from pathlib import Path


class constants(Enum):
    DATASET_ID = "us_fdic_bankfind"

    # The static tables are rebuilt whole on every run: the institution
    # directory is a current-state snapshot and the indicator dictionary is
    # derived from the FDIC's published property files.  Both are small.
    STATIC_TABLES = ["institution", "indicator"]
    QUARTERLY_TABLES = ["financials", "financials_indicator"]
    ALL_TABLES = [
        "institution",
        "indicator",
        "financials",
        "financials_indicator",
    ]

    # The table whose coverage drives the source poll.
    POLL_TABLE = "financials"

    # How many recent quarters to rebuild each run.  Institutions file amended
    # Call Reports for several quarters after the fact, so refreshing only the
    # newest quarter would leave those revisions behind.  Each quarter writes
    # `year=YYYY/data_q<N>.parquet`, so a rebuild replaces that object rather
    # than adding a second copy -- which is what makes a re-run idempotent and
    # lets the upload stay on dump_mode="append".
    TRAILING_QUARTERS = 2

    ARCHITECTURE_DIR = str(
        Path(__file__).resolve().parents[3]
        / "models/us_fdic_bankfind/code/architecture"
    )
    CODE_DIR = str(
        Path(__file__).resolve().parents[3] / "models/us_fdic_bankfind/code"
    )
