"""Scratch paths for the us_eia_electricity onboarding, plus the shared transform.

The cleaning transform lives in ``pipelines/datasets/us_eia_electricity/utils.py`` and is
imported here rather than duplicated, so the one-shot bootstrap and the recurring
pipeline can never drift. This module only adds what the bootstrap needs on top:
where the scratch data lives.

Scratch data goes under ``~/Downloads/us_eia_electricity_data/`` (never in the repo or in
Dropbox), overridable via ``US_EIA_ELECTRICITY_DATA_DIR``. The architecture CSVs under
``architecture/`` remain the source of truth for column names, order, types and
the PUDL-canonical name each column is read from; ``gen_architecture.py`` writes
them.
"""

import os
import sys
from pathlib import Path

CODE_DIR = Path(__file__).resolve().parent
REPO_ROOT = CODE_DIR.parents[2]
# These scripts run from their own directory with bare sibling imports, so the
# repo root is not otherwise importable.
sys.path.insert(0, str(REPO_ROOT))

from pipelines.datasets.us_eia_electricity.constants import (  # noqa: E402
    constants,
)
from pipelines.datasets.us_eia_electricity.utils import (  # noqa: E402,F401
    BUILDERS,
    TABLE_FORM,
    Col,
    YearReader,
    assert_all_string,
    build_dicionario,
    clean_all,
    clean_year,
    codes,
    county_id_for,
    data_maturity,
    download_form,
    list_source_files,
    load_cols,
    repair_code,
    source_max_date,
    source_zip,
    state_id_for,
)

DATA_DIR = Path(
    os.environ.get(
        "US_EIA_ELECTRICITY_DATA_DIR",
        Path.home() / "Downloads" / "us_eia_electricity_data",
    )
)
INPUT = DATA_DIR / "input"
OUTPUT = DATA_DIR / "output"

DATASET_ID = constants.DATASET_ID.value
ALL_TABLES = constants.ALL_TABLES.value
DATA_TABLES = constants.DATA_TABLES.value
