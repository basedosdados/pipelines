"""Constants for the world_cricsheet recurring pipeline (Prefect 3).

Cricsheet global cricket. The full-history bundle ``all_csv2.zip`` ships the
entire history on every (near-daily) release, so each run is a full replace
(``dump_mode="overwrite"``). Because that rebuilds the whole 11.4M-row dataset,
the pipeline runs **weekly** rather than daily (see ``flows.py`` schedule). See
models/world_cricsheet/ for the onboarding design and the architecture CSVs (the
schema source of truth).
"""

from enum import Enum
from pathlib import Path

# Repo root, then the committed architecture CSVs (column order + bigquery_type
# per table — the single schema source of truth, shared with the one-shot
# bootstrap in models/world_cricsheet/code/).
_REPO_ROOT = Path(__file__).resolve().parents[3]


class constants(Enum):
    """Constants for the world_cricsheet pipeline.

    Lowercase class name follows the repo-wide convention for dataset constant
    enums.
    """

    DATASET_ID = "world_cricsheet"

    # Full-history bundle (ships the whole history each release) + the person
    # registry. The bundle does NOT contain the pre-concatenated all_matches.csv
    # (only the per-competition bundles do), so the downloader builds it by
    # concatenating the per-match ball files after extraction.
    ALL_CSV2_URL = "https://cricsheet.org/downloads/all_csv2.zip"
    PEOPLE_URL = "https://cricsheet.org/register/people.csv"

    # The primary raw data source's URL, used as the poll/commit selector: all
    # four tables are linked to three raw sources (bundle + people + names), and
    # the poll/commit client raises on an ambiguous (2+) source unless told which
    # one by exact URL. This is the bundle that actually drives the refresh.
    BUNDLE_RAW_SOURCE_URL = ALL_CSV2_URL

    # cricsheet.org serves plainly, but send a browser-like UA with a contact so
    # the near-daily automated fetch is identifiable and not rate-limited.
    USER_AGENT = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120 Safari/537.36 rdahis@basedosdados.org"
    )

    # Upload/materialize order: smallest first so a failure surfaces cheaply.
    ALL_TABLES = ["people", "match_players", "matches", "deliveries"]

    # The table the flow polls + commits the source Update against. matches is a
    # part_bdpro table carrying start_date; polling it (rather than the 11.4M-row
    # deliveries) is cheap and its source coverage is identical.
    POLL_TABLE = "matches"

    ARCHITECTURE_DIR = (
        _REPO_ROOT / "models" / "world_cricsheet" / "code" / "architecture"
    )
