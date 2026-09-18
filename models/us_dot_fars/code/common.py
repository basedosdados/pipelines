"""Scratch paths for the us_dot_fars onboarding, plus the shared transform.

The cleaning transform lives in ``pipelines/datasets/us_dot_fars/utils.py`` and
is imported here rather than duplicated, so the one-shot bootstrap and the
recurring pipeline can never drift. This module adds only what the bootstrap
needs on top: where the scratch data lives.

Scratch data goes under ``~/Downloads/us_dot_fars_data/`` (never in the repo or
in Dropbox), overridable via ``FARS_DATA_DIR``. The architecture CSVs under
``architecture/`` remain the source of truth for column names, order, types and
the raw -> clean mapping; ``gen_architecture.py`` writes them.
"""

import os
import sys
from pathlib import Path

CODE_DIR = Path(__file__).resolve().parent
REPO_ROOT = CODE_DIR.parents[2]
# These scripts run from their own directory with bare sibling imports, so the
# repo root is not otherwise importable.
sys.path.insert(0, str(REPO_ROOT))

from pipelines.datasets.us_dot_fars.constants import constants  # noqa: E402
from pipelines.datasets.us_dot_fars.utils import (  # noqa: E402,F401
    Col,
    assert_all_string,
    blood_alcohol,
    build_dicionario,
    clean_all,
    clean_table,
    clean_year,
    code,
    codebook_for_year,
    coord,
    county_id_for,
    download_year,
    latest_published_year,
    load_cols,
    model_year,
    read_rows,
    source_max_date,
    write_partition,
    year_is_published,
)

DATA_DIR = Path(
    os.environ.get(
        "FARS_DATA_DIR", Path.home() / "Downloads" / "us_dot_fars_data"
    )
)
INPUT = DATA_DIR / "input"
OUTPUT = DATA_DIR / "output"

DATASET_ID = constants.DATASET_ID.value
DATA_TABLES = constants.DATA_TABLES.value
ALL_TABLES = constants.ALL_TABLES.value
FIRST_YEAR = constants.FIRST_YEAR.value
