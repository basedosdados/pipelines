"""Scratch paths for the world_iati_activities onboarding, plus the shared transform.

The cleaning transform lives in ``pipelines/datasets/world_iati_activities/utils.py``
and is imported here rather than duplicated, so the one-shot bootstrap and the
recurring pipeline can never drift. This module only adds what the bootstrap
needs on top: where the scratch data lives.

Scratch data goes under ``~/Downloads/world_iati_activities_data/`` (never in the
repo or in Dropbox), overridable via ``IATI_DATA_DIR``. The architecture CSVs
under ``architecture/`` remain the source of truth for column names, order, types
and the raw -> clean name mapping; ``gen_architecture.py`` writes them.
"""

import os
import sys
from pathlib import Path

CODE_DIR = Path(__file__).resolve().parent
ARCH_DIR = CODE_DIR / "architecture"
REPO_ROOT = CODE_DIR.parents[2]
# These scripts run from their own directory with bare sibling imports, so the
# repo root is not otherwise importable.
sys.path.insert(0, str(REPO_ROOT))

DATA_DIR = Path(
    os.environ.get(
        "IATI_DATA_DIR",
        Path.home() / "Downloads" / "world_iati_activities_data",
    )
)
INPUT = DATA_DIR / "input"
OUTPUT = DATA_DIR / "output"
# The per-table CSVs extracted from iati_csv.zip. Deleted table by table as each
# is converted, so peak disk stays near the largest single table rather than the
# whole 30 GB corpus.
CSV_DIR = DATA_DIR / "csv"

CSV_ZIP = INPUT / "iati_csv.zip"
DATASETS_MINIMAL = INPUT / "datasets_minimal.json"
STATS = INPUT / "stats.json"
