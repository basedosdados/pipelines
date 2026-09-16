"""Scratch paths for the us_eia_consumption onboarding, plus the shared transform.

The cleaning transform lives in ``pipelines/datasets/us_eia_consumption/utils.py``
and is imported here rather than duplicated. Scratch data goes under
``~/Downloads/us_eia_consumption_data/`` (never in the repo or Dropbox),
overridable via ``US_EIA_CONSUMPTION_DATA_DIR``.
"""

import os
import sys
from pathlib import Path

CODE_DIR = Path(__file__).resolve().parent
REPO_ROOT = CODE_DIR.parents[2]
sys.path.insert(0, str(REPO_ROOT))

from pipelines.datasets.us_eia_consumption.constants import (  # noqa: E402
    constants,
)
from pipelines.datasets.us_eia_consumption.utils import (  # noqa: E402,F401
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
