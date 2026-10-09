"""Scratch paths for the us_eia_consumption onboarding, plus the shared transform.

The cleaning transform lives in ``pipelines/datasets/us_eia_consumption/utils.py``
and is imported here rather than duplicated. Scratch data goes under
``~/Downloads/us_eia_consumption_data/`` (never in the repo or Dropbox),
overridable via ``US_EIA_CONSUMPTION_DATA_DIR``.
"""

import os
from pathlib import Path

from pipelines.datasets.us_eia_consumption.constants import (
    constants,
)
from pipelines.datasets.us_eia_consumption.utils import (  # noqa: F401
    Col,
    assert_all_string,
    build_dicionario,
    clean_all,
    clean_annual_year,
    clean_eia861m,
    county_id_for,
    download_eia861m,
    download_year,
    load_cols,
    source_max_annual_year,
    state_id_for,
    year_zip,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
ARCHITECTURE_DIR = Path(constants.ARCHITECTURE_DIR.value)

DATA_DIR = Path(
    os.environ.get(
        "US_EIA_CONSUMPTION_DATA_DIR",
        Path.home() / "Downloads" / "us_eia_consumption_data",
    )
)
INPUT = DATA_DIR / "input"
OUTPUT = DATA_DIR / "output"

DATASET_ID = constants.DATASET_ID.value
ALL_TABLES = constants.ALL_TABLES.value
DATA_TABLES = constants.DATA_TABLES.value
ANNUAL_TABLES = constants.ANNUAL_TABLES.value
