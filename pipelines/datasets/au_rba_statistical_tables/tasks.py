"""Prefect 3 tasks for au_rba_statistical_tables — thin wrappers over utils.py,
plus the check_update entry point the staged pipeline dispatches through (see
the module banner in flows.py).
"""

import shutil
import tempfile
from datetime import date
from pathlib import Path

from prefect import task

from pipelines.datasets.au_rba_statistical_tables.utils import (
    clean_all,
    download_all,
    write_partitioned,
)
from pipelines.utils.stage_dispatch import SourceInspection


@task(retries=2, retry_delay_seconds=30)
def download_rba(work_dir: str) -> str:
    """Download every RBA statistical-table CSV.

    Retries twice: www.rba.gov.au occasionally drops a connection partway
    through the ~220-file sweep.

    Args:
        work_dir: Directory to download into; files land in ``<work_dir>/input``.

    Returns:
        The input directory path, as a string (Prefect serializes task results).
    """
    input_dir = Path(work_dir) / "input"
    saved = download_all(input_dir)
    print(f"downloaded {len(saved)} CSV files into {input_dir}")
    return str(input_dir)


@task
def clean_rba(work_dir: str, input_dir: str) -> dict:
    """Build the four tables from the downloaded CSVs.

    Args:
        work_dir: Directory to write into; tables land under ``<work_dir>/output``.
        input_dir: Directory holding the downloaded CSVs, from :func:`download_rba`.

    Returns:
        A mapping of table slug to its output directory, plus
        ``"max_publication_date"`` — the latest ``"YYYY-MM-DD"`` publication
        stamp across all series, which drives the source-update poll.
    """
    output_dir = Path(work_dir) / "output"
    cleaned = clean_all(Path(input_dir))
    counts = write_partitioned(cleaned, output_dir)

    for table, n in counts.items():
        print(f"  {table}: {n:,} rows")
    print(f"  excluded by licence gate: {len(cleaned['excluded']):,} series")

    result = {table: str(output_dir / table) for table in counts}
    result["max_publication_date"] = cleaned["max_publication_date"]
    return result


# ──────────────────────────────────────────────────────────────────────────────
# get_latest_update — the staged pipeline's check_update entry point.
#
# There is no cheap freshness signal for this source: the RBA's "Publication
# date" stamp lives inside each CSV's own metadata block, not in any index page
# or HTTP header, so discovering it requires downloading and parsing every
# file — exactly the same work `extract_and_load` does. A "check_and_download"
# variant that passed the already-downloaded CSVs through to extract_and_load
# was considered and dropped: every dataset in this migration uses the same
# check_update -> extract_and_load shape, even when check_update has to
# download data to find the reference date. `extract_load_data` (via
# `clean_rba` in flows.py) downloads and parses again, independently — a
# duplicated ~220-file download+parse pass accepted in exchange for one
# uniform pipeline shape across the repo.
# ──────────────────────────────────────────────────────────────────────────────


def get_latest_update() -> SourceInspection:
    """Download every CSV and parse it, solely to read the freshness stamp.

    See the module banner above for why this re-does the same download+parse
    pass that `extract_and_load` performs on its own, independently.

    Returns:
        `SourceInspection` with the max "Publication date" across all kept
        series as `reference_date`. Compared against `Table.Update.latest`
        (`compare_against="table_update"`), not the default `Coverage` check:
        the newest coverage date belongs to a forward-dated quarterly
        expectations series (G3), months ahead of the daily data, which would
        stall the poll against a plain coverage baseline.
    """
    work_dir = tempfile.mkdtemp(prefix="au_rba_statistical_tables_check_")
    try:
        input_dir = download_rba(work_dir=work_dir)
        cleaned = clean_all(Path(input_dir))
        max_pub = cleaned["max_publication_date"]
        return SourceInspection(
            reference_date=date.fromisoformat(max_pub),
            compare_against="table_update",
        )
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)
