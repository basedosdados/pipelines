"""Prefect tasks for us_fec_campaign_finance — one independent check_update /
extract_and_load pair per table (see the module banner in flows.py).

Every FEC bulk file type is its own ZIP (``utils.SPECS``), so there is nothing
shared to decompose: ``make_extract_load_data`` always downloads and parses just
its own table, same as the monolithic flow did before the split.

Two check strategies, picked per table (see ``CONTENT_TABLES``):

* The four transaction tables: ``get_latest_update`` downloads and parses the
  current cycle for just that table, to read the real max ``transaction_date``
  out of the content — the FEC republishes the whole current cycle daily
  regardless of whether new rows actually landed, so a cheap Last-Modified HEAD
  can't tell "republished" from "actually new data" apart. ``extract_load_data``
  repeats the same download+parse independently right after — duplicated cost,
  accepted house-wide so check_update and extract_and_load stay cleanly
  separated even on a dataset this heavy.
* The three registration snapshots (candidate, committee,
  candidate_committee_link): their Coverage is year-only (the two-year election
  cycle), which does not move within a cycle, so comparing against it would
  silently stop refreshing them after the first run of each cycle.
  ``get_latest_update`` instead HEADs the table's own bulk ZIP and compares its
  Last-Modified against ``Table.Update.latest``
  (``compare_against="table_update"``) — cheaper besides, since it never
  downloads during the check.
"""

import shutil
import tempfile
from collections.abc import Callable
from datetime import date
from pathlib import Path

from pipelines.datasets.us_fec_campaign_finance.constants import constants
from pipelines.datasets.us_fec_campaign_finance.utils import (
    SPECS,
    current_cycle,
    refresh_cycle,
    source_last_modified,
)
from pipelines.utils.stage_dispatch import (
    ExtractAndLoad,
    SourceInspection,
    pipeline_factory,
)

DATASET_ID = constants.DATASET_ID.value

# The four high-frequency transaction tables — see the module banner for why they
# check differently from the three registration snapshots.
CONTENT_TABLES = frozenset(
    {
        "contribution_individual",
        "contribution_committee",
        "committee_transaction",
        "disbursement",
    }
)


def _refresh_one(table_id: str, work_dir: Path) -> dict:
    """Download and clean the current cycle for just `table_id`.

    Returns ``{table_id: <dir to upload>}`` plus ``max_date``, both from
    ``refresh_cycle`` (see utils.py) — ``{"max_date": None}`` alone when the FEC
    hasn't published this cycle's file yet for this table.
    """
    cycle = current_cycle(date.today())
    return refresh_cycle(cycle=cycle, work_dir=work_dir, tables=[table_id])


def make_get_latest_update(table_id: str) -> Callable[[], SourceInspection]:
    """Build `table_id`'s check_update logic — see the module banner."""
    if table_id in CONTENT_TABLES:

        def get_latest_update() -> SourceInspection:
            work_dir = Path(
                tempfile.mkdtemp(
                    prefix=f"us_fec_campaign_finance_{table_id}_check_"
                )
            )
            try:
                result = _refresh_one(table_id, work_dir)
                max_date = result.get("max_date")
                if max_date is None:
                    # Not published yet for this cycle (e.g. right after the
                    # cycle rolled over) — report a date old enough to never
                    # look newer than the registered Coverage, so the poll is
                    # a clean "nothing new" instead of crashing the scheduled
                    # check.
                    return SourceInspection(reference_date=date.min)
                return SourceInspection(
                    reference_date=date.fromisoformat(max_date)
                )
            finally:
                shutil.rmtree(work_dir, ignore_errors=True)

        return get_latest_update

    spec = SPECS[table_id]

    def get_latest_update() -> SourceInspection:
        cycle = current_cycle(date.today())
        published = source_last_modified(spec, cycle)
        return SourceInspection(
            reference_date=date.fromisoformat(published),
            compare_against="table_update",
        )

    return get_latest_update


def make_extract_load_data(table_id: str) -> Callable[[dict], ExtractAndLoad]:
    """Build `table_id`'s extract_and_load logic — see the module banner."""

    def extract_load_data(download_params: dict) -> ExtractAndLoad:
        # Not cleaned up afterwards (no `shutil.rmtree`/`finally`): `data_path`
        # is read by `upload_to_gcs` right after this returns, inside the same
        # flow run — same trade-off as au_ato_abr.tasks.make_extract_load_data.
        work_dir = Path(
            tempfile.mkdtemp(prefix=f"us_fec_campaign_finance_{table_id}_")
        )
        result = _refresh_one(table_id, work_dir)
        return ExtractAndLoad(
            data_path=result[table_id],
            dump_mode="append",
            source_format="parquet",
        )

    return extract_load_data


make_pipeline = pipeline_factory(
    DATASET_ID,
    make_get_latest_update,
    make_extract_load_data,
)
