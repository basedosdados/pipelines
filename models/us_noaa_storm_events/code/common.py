"""Scratch paths for the us_noaa_storm_events onboarding, plus the shared transform.

The cleaning transform lives in ``pipelines/datasets/us_noaa_storm_events/utils.py``
and is imported here rather than duplicated, so the one-shot bootstrap and the
recurring pipeline can never drift. This module only adds what the bootstrap needs
on top: where the scratch data lives.

Scratch data goes under ``~/Downloads/us_noaa_storm_events_data/`` (never in the
repo or in Dropbox), overridable via ``NOAA_STORM_EVENTS_DATA_DIR``. The
architecture CSVs under ``architecture/`` remain the source of truth for column
names, order, types and the raw -> clean name mapping; ``gen_architecture.py``
writes them.
"""

import os
import sys
from pathlib import Path

CODE_DIR = Path(__file__).resolve().parent
REPO_ROOT = CODE_DIR.parents[2]
# These scripts run from their own directory with bare sibling imports, so the
# repo root is not otherwise importable.
sys.path.insert(0, str(REPO_ROOT))

from pipelines.datasets.us_noaa_storm_events.constants import (  # noqa: E402
    constants,
)
from pipelines.datasets.us_noaa_storm_events.utils import (  # noqa: E402,F401
    Col,
    assert_all_string,
    build_dicionario,
    clean_all,
    clean_event,
    clean_event_location,
    clean_fatality,
    clean_year,
    county_id_for,
    download_years,
    list_source_files,
    load_cols,
    parse_damage,
    read_rows,
    source_max_date,
    state_id_for,
)

DATA_DIR = Path(
    os.environ.get(
        "NOAA_STORM_EVENTS_DATA_DIR",
        Path.home() / "Downloads" / "us_noaa_storm_events_data",
    )
)
INPUT = DATA_DIR / "input"
OUTPUT = DATA_DIR / "output"

DATASET_ID = constants.DATASET_ID.value
ALL_TABLES = constants.ALL_TABLES.value
DATA_TABLES = ["event", "fatality", "event_location"]
