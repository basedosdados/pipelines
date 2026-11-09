"""Prefect 3 tasks for world_cricsheet — thin wrappers over utils.py, plus the
`get_latest_update` entry point the staged pipeline's check_update dispatches
through (see the module banner in flows.py for why there is no per-table
split here).
"""

import shutil
import tempfile
from datetime import date
from pathlib import Path

from prefect import task

from pipelines.datasets.world_cricsheet.utils import clean_all, download_bundle
from pipelines.utils.stage_dispatch import SourceInspection


@task(retries=2, retry_delay_seconds=60)
def download_cricsheet(work_dir: str) -> str:
    """Download + stage the Cricsheet bundle and person registry.

    Retries twice: the ~114 MB bundle download occasionally drops.

    Args:
        work_dir: Directory to download into; files land in ``<work_dir>/input``.

    Returns:
        The input directory path, as a string (Prefect serializes task results).
    """
    input_dir = Path(work_dir) / "input"
    download_bundle(input_dir)
    return str(input_dir)


@task
def clean_cricsheet(work_dir: str, input_dir: str) -> dict:
    """Build the four partitioned tables from the staged bundle.

    Args:
        work_dir: Directory to write into; tables land under ``<work_dir>/output``.
        input_dir: Staged bundle directory, from :func:`download_cricsheet`.

    Returns:
        A mapping of table slug to its partitioned output directory (as strings),
        plus ``"max_start_date"`` — the latest match ``"YYYY-MM-DD"`` in the
        bundle, which drives the source-update poll — and ``"counts"``.
    """
    output_dir = Path(work_dir) / "output"
    result = clean_all(Path(input_dir), output_dir)
    return {
        k: (str(v) if isinstance(v, Path) else v) for k, v in result.items()
    }


# ──────────────────────────────────────────────────────────────────────────────
# get_latest_update — the staged pipeline's check_update entry point.
#
# There is no cheap freshness signal for this source: the only place a match
# date lives is inside the full bundle itself (there is no index page or HTTP
# header carrying it), so discovering it requires downloading and rebuilding
# the whole thing — exactly the same work `extract_and_load` does. A
# "check_and_download" variant that passed the already-downloaded bundle
# through to extract_and_load was considered and dropped: every dataset in
# this migration uses the same check_update -> extract_and_load shape, even
# when check_update has to download data to find the reference date.
# `extract_and_load` (via `download_cricsheet`/`clean_cricsheet` in flows.py)
# downloads and rebuilds again, independently — a duplicated ~114 MB
# download + full rebuild pass accepted in exchange for one uniform pipeline
# shape across the repo.
#
# One `get_latest_update`/one `extract_and_load` for the whole dataset, not a
# pair per table (the repo-wide one-pipeline-per-table_id shape does NOT apply
# here): `clean_all` builds all four tables from a single non-decomposable
# bundle download in one pass, AND `world_cricsheet__match_players.person_id`
# carries a `relationships` test against `ref('world_cricsheet__people')`
# (see models/world_cricsheet/schema.yml) — a real cross-table dbt dependency,
# the one case the repo-wide rule carves out. Same shape as
# `br_bcb_ifdata`/`au_rba_statistical_tables`: see the module banner in
# flows.py for how materialization is batched to keep `run` of every table
# ahead of `test` of any table.
# ──────────────────────────────────────────────────────────────────────────────


def get_latest_update() -> SourceInspection:
    """Download the full bundle and rebuild it, solely to read the freshest match date.

    See the module banner above for why this re-does the same download +
    rebuild pass that `extract_and_load` performs on its own, independently.

    Returns:
        `SourceInspection` with the latest match `start_date` across the
        bundle as `reference_date`. Compared against `Coverage.DateTimeRange`
        (`compare_against="coverage"`, the default) via the `matches` table,
        which carries that column — same poll target as before the staged
        split.

    Raises:
        ValueError: If the bundle has no parseable match date at all (every
            real Cricsheet release has one; this would signal a corrupt or
            empty download).
    """
    work_dir = tempfile.mkdtemp(prefix="world_cricsheet_check_")
    try:
        input_dir = download_cricsheet(work_dir=work_dir)
        result = clean_cricsheet(work_dir=work_dir, input_dir=input_dir)
        max_start = result["max_start_date"]
        if max_start is None:
            raise ValueError(
                "world_cricsheet: no parseable match date found in bundle"
            )
        return SourceInspection(
            reference_date=date.fromisoformat(max_start),
            compare_against="coverage",
        )
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)
