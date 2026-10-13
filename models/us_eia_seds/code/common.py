"""Scratch paths for the us_eia_seds onboarding, plus the shared transform.

The cleaning transform lives in ``pipelines/datasets/us_eia_seds/utils.py`` and is
imported here rather than duplicated, so the one-shot bootstrap and the recurring
pipeline can never drift. This module only adds where the scratch data lives.

Scratch data goes under ``~/Downloads/us_eia_seds_data/`` (never in the repo or in
Dropbox), overridable via ``US_EIA_SEDS_DATA_DIR``. The architecture CSV under
``architecture/`` is the schema source of truth; ``gen_architecture.py`` writes it.
"""

import os
from pathlib import Path

from pipelines.datasets.us_eia_seds.constants import constants
from pipelines.datasets.us_eia_seds.utils import (  # noqa: F401
    Col,
    assert_all_string,
    build_dicionario,
    build_seds_consumption,
    clean_all,
    clean_year,
    download_codes,
    download_complete,
    load_cols,
    load_msn_codes,
    load_state_codes,
    read_complete,
    source_max_date,
    state_id_for,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
ARCHITECTURE_DIR = Path(constants.ARCHITECTURE_DIR.value)

DATA_DIR = Path(
    os.environ.get(
        "US_EIA_SEDS_DATA_DIR", Path.home() / "Downloads" / "us_eia_seds_data"
    )
)
INPUT = DATA_DIR / "input"
OUTPUT = DATA_DIR / "output"

DATASET_ID = constants.DATASET_ID.value
ALL_TABLES = constants.ALL_TABLES.value
DATA_TABLES = constants.DATA_TABLES.value
