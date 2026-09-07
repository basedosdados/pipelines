"""Scratch paths for the us_cfpb_complaints onboarding, plus the shared transform.

The cleaning transform itself lives in ``pipelines/datasets/us_cfpb_complaints/utils.py``
and is imported here rather than duplicated, so the one-shot bootstrap and the
recurring pipeline can never drift. This module only adds what the bootstrap needs on
top: where the scratch data lives.

Scratch data goes under ``~/Downloads/us_cfpb_complaints_data/`` (never in the repo or
in Dropbox), overridable via ``CFPB_COMPLAINTS_DATA_DIR``. The architecture TSVs in
this directory remain the source of truth for column names, order, types and the
raw -> clean name mapping; ``gen_architecture.py`` writes them.
"""

import os
import sys
from pathlib import Path

CODE_DIR = Path(__file__).resolve().parent
REPO_ROOT = CODE_DIR.parents[2]
# These scripts run from their own directory with bare sibling imports, so the repo
# root is not otherwise importable.
sys.path.insert(0, str(REPO_ROOT))

from pipelines.datasets.us_cfpb_complaints.constants import (  # noqa: E402
    constants,
)
from pipelines.datasets.us_cfpb_complaints.utils import (  # noqa: E402,F401
    Col,
    assert_all_string,
    build_dicionario,
    clean_all,
    clean_complaint,
    download_snapshot,
    load_cols,
    state_id_for,
)

DATA_DIR = Path(
    os.environ.get(
        "CFPB_COMPLAINTS_DATA_DIR",
        Path.home() / "Downloads" / "us_cfpb_complaints_data",
    )
)
INPUT = DATA_DIR / "input"
OUTPUT = DATA_DIR / "output"

DATASET_ID = constants.DATASET_ID.value
COMPLAINT = constants.COMPLAINT.value
DICIONARIO = constants.DICIONARIO.value

CSV_NAME = constants.CSV_NAME.value
ZIP_NAME = constants.ZIP_NAME.value
BULK_URL = constants.BULK_URL.value
DICT_COLUMNS = constants.DICT_COLUMNS.value
STATE_FIPS = constants.STATE_FIPS.value
STATE_ALIASES = constants.STATE_ALIASES.value
