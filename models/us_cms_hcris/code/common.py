"""Scratch paths for the us_cms_hcris onboarding, plus the shared transform.

The download and cleaning functions live in
``pipelines/datasets/us_cms_hcris/utils.py`` and are imported here rather than
duplicated, so the one-shot bootstrap and the recurring pipeline can never
drift. This module only adds where the scratch data lives.

Scratch goes under ``~/Downloads/us_cms_hcris_data/`` — never in the repo or in
Dropbox — overridable via ``US_CMS_HCRIS_DATA_DIR``. The architecture CSVs under
``architecture/`` remain the source of truth for column names, order and types;
``gen_architecture.py`` writes them.
"""

import os
import sys
from pathlib import Path

CODE_DIR = Path(__file__).resolve().parent
REPO_ROOT = CODE_DIR.parents[2]
# These scripts run from their own directory with bare sibling imports, so the
# repo root is not otherwise importable.
sys.path.insert(0, str(REPO_ROOT))

from pipelines.datasets.us_cms_hcris.constants import constants  # noqa: E402
from pipelines.datasets.us_cms_hcris.utils import (  # noqa: E402,F401
    FORM_TAG,
    REPORT_COLUMNS,
    REPORT_VALUE_COLUMNS,
    assert_all_string,
    clean_all,
    clean_extract,
    connect,
    download_extract,
    extract_exists,
    list_extracts,
    unpack_extract,
    zip_url,
)

DATA_DIR = Path(
    os.environ.get(
        "US_CMS_HCRIS_DATA_DIR",
        Path.home() / "Downloads" / "us_cms_hcris_data",
    )
)
INPUT = DATA_DIR / "input"
OUTPUT = DATA_DIR / "output"
DOCS = DATA_DIR / "docs"

DATASET_ID = constants.DATASET_ID.value
ALL_TABLES = constants.ALL_TABLES.value
STAGED_TABLES = constants.STAGED_TABLES.value

# The extract series this onboarding was built and verified against. The
# pipeline probes for new years rather than reading this; it is here so the
# bootstrap is reproducible.
EXTRACTS = [("2552-96", y) for y in range(1996, 2012)] + [
    ("2552-10", y) for y in range(2010, 2027)
]
