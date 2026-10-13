"""Scratch paths for the us_census_cog onboarding, plus the shared transform.

The cleaning transform lives in ``pipelines/datasets/us_census_cog/utils.py``
and is imported here rather than duplicated, so the one-shot bootstrap and the
recurring pipeline can never drift.

Scratch data goes under ``~/Downloads/us_census_cog_data/`` (never in the repo
or in Dropbox), overridable via ``CENSUS_COG_DATA_DIR``. The architecture CSVs
under ``architecture/`` remain the source of truth for column names, order,
types and the raw -> clean name mapping.
"""

import os
from pathlib import Path

# K is re-exported for the scripts in this directory, which read the per-year
# source listings from it. ruff sees no use of it here and would strip it.
from pipelines.datasets.us_census_cog import (  # noqa: F401, N812
    constants as K,
)
from pipelines.datasets.us_census_cog.constants import constants

DATA_DIR = Path(
    os.environ.get(
        "CENSUS_COG_DATA_DIR",
        Path.home() / "Downloads" / "us_census_cog_data",
    )
)
INPUT = DATA_DIR / "input"
OUTPUT = DATA_DIR / "output"

DATASET_ID = constants.DATASET_ID.value
ALL_TABLES = constants.ALL_TABLES.value
DATA_TABLES = constants.DATA_TABLES.value
ARCHITECTURE = Path(constants.ARCHITECTURE_DIR.value)
