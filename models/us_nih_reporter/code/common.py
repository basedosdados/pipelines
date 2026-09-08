"""Scratch paths for the us_nih_reporter onboarding, plus the shared transform.

The cleaning transform lives in ``pipelines/datasets/us_nih_reporter/utils.py``
and is imported here rather than duplicated, so the one-shot bootstrap and the
recurring pipeline can never drift. This module only adds what the bootstrap
needs on top: where the scratch data lives.

Scratch data goes under ``~/Downloads/us_nih_reporter_data/`` (never in the repo
or in Dropbox), overridable via ``NIH_REPORTER_DATA_DIR``. The architecture CSVs
under ``architecture/`` remain the source of truth for column names, order,
types and the raw -> clean name mapping; ``gen_architecture.py`` writes them.
"""

import os
import sys
from pathlib import Path

CODE_DIR = Path(__file__).resolve().parent
REPO_ROOT = CODE_DIR.parents[2]
# These scripts run from their own directory with bare sibling imports, so the
# repo root is not otherwise importable.
sys.path.insert(0, str(REPO_ROOT))

from pipelines.datasets.us_nih_reporter.constants import (  # noqa: E402
    constants,
)
from pipelines.datasets.us_nih_reporter.utils import (  # noqa: E402,F401
    Col,
    assert_all_string,
    build_dicionario,
    clean_abstract_year,
    clean_all,
    clean_clinical_studies,
    clean_patents,
    clean_project_year,
    clean_publication_link_year,
    clean_publication_year,
    download_family,
    download_funding_supplement,
    load_activity_code_labels,
    load_cols,
    load_funding_supplement,
    norm_date,
    norm_float,
    norm_int,
    read_rows,
    source_filename,
    source_max_date,
    source_url,
    write_flat,
    write_partition,
)

DATA_DIR = Path(
    os.environ.get(
        "NIH_REPORTER_DATA_DIR",
        Path.home() / "Downloads" / "us_nih_reporter_data",
    )
)
INPUT = DATA_DIR / "input" / "raw"
OUTPUT = DATA_DIR / "output"

DATASET_ID = constants.DATASET_ID.value
ALL_TABLES = constants.ALL_TABLES.value
PARTITIONED_TABLES = constants.PARTITIONED_TABLES.value
DATA_TABLES = [t for t in ALL_TABLES if t != "dicionario"]

# The full published corpus, as of the FY2025 project release.
FISCAL_YEARS = list(range(constants.FIRST_FISCAL_YEAR.value, 2026))
CALENDAR_YEARS = list(range(constants.FIRST_CALENDAR_YEAR.value, 2026))
