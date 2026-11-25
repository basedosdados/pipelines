"""Prefect 3 tasks for world_wb_wdi — thin wrappers over utils.py, plus the
check_update entry point the staged pipeline dispatches through (see the
module banner in flows.py).
"""

import shutil
import tempfile
from datetime import date
from pathlib import Path

from prefect import task

from pipelines.datasets.world_wb_wdi.utils import (
    build_data,
    clean_all,
    download_source,
)
from pipelines.utils.stage_dispatch import SourceInspection


@task(retries=2, retry_delay_seconds=60)
def download_wdi(work_dir: str) -> str:
    """Download and extract WDI_CSV.zip from the World Bank.

    Retries twice: the ~270MB archive occasionally drops mid-download.

    Args:
        work_dir: Directory to download into; CSVs land in ``<work_dir>/input``.

    Returns:
        The input directory path, as a string (Prefect serializes task results).
    """
    input_dir = Path(work_dir) / "input"
    download_source(input_dir)
    return str(input_dir)


@task
def clean_wdi(work_dir: str, input_dir: str) -> dict:
    """Build the six partitioned tables from the extracted WDI CSVs.

    Args:
        work_dir: Directory to write into; tables land under ``<work_dir>/output``.
        input_dir: Directory holding the extracted CSVs, from :func:`download_wdi`.

    Returns:
        A mapping of table slug to its output directory (as strings), plus
        ``"max_year"`` — the latest year in ``data``, which drives the
        source-update poll — and ``"counts"``.
    """
    output_dir = Path(work_dir) / "output"
    result = clean_all(Path(input_dir), output_dir)
    return {
        k: (str(v) if isinstance(v, Path) else v) for k, v in result.items()
    }


# ──────────────────────────────────────────────────────────────────────────────
# get_latest_update — the staged pipeline's check_update entry point.
#
# There is no cheap freshness signal for this source: the only place the
# latest year appears is inside the ~270MB bulk archive itself (no index page
# or HTTP header carries it), so discovering it requires downloading and
# melting the same WDICSV.csv that `extract_and_load` processes again. A
# "check_and_download" variant that passed the already-downloaded CSVs
# through to extract_and_load was considered and dropped: every dataset in
# this migration uses the same check_update -> extract_and_load shape, even
# when check_update has to download data to find the reference date.
# `extract_load_data` (via `download_wdi`/`clean_wdi` in flows.py) downloads
# and melts again, independently — a duplicated ~270MB download+melt pass
# accepted in exchange for one uniform pipeline shape across the repo.
# ──────────────────────────────────────────────────────────────────────────────


def get_latest_update() -> SourceInspection:
    """Download the WDI archive and melt ``data``, solely to read the latest year.

    Melts only ``WDICSV.csv`` (:func:`build_data`) rather than running
    `clean_all`'s full six-table build — the other five tables don't carry the
    freshness signal, and this step writes nothing to disk, unlike
    `extract_and_load`'s `clean_wdi`, which materializes every table as
    parquet. See the module banner above for why this still re-downloads and
    re-melts the same CSV that `extract_and_load` processes again.

    Returns:
        `SourceInspection` with the latest year present in ``data`` as
        `reference_date` (Jan 1st of that year). Compared against the default
        `Coverage` baseline (``compare_against="coverage"``) — unlike
        `au_rba_statistical_tables`, ``data``'s `YearOnly` coverage tracks
        this value directly, so no forward-dated series forces
        `"table_update"` here.
    """
    work_dir = tempfile.mkdtemp(prefix="world_wb_wdi_check_")
    try:
        input_dir = download_wdi(work_dir=work_dir)
        max_year = int(build_data(Path(input_dir))["year"].max())
        return SourceInspection(reference_date=date(max_year, 1, 1))
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)
