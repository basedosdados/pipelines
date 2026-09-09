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
import sys
from pathlib import Path

CODE_DIR = Path(__file__).resolve().parent
REPO_ROOT = CODE_DIR.parents[2]
# These scripts run from their own directory with bare sibling imports, so the
# repo root is not otherwise importable.
sys.path.insert(0, str(REPO_ROOT))

from pipelines.datasets.us_census_cog.constants import constants  # noqa: E402

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
