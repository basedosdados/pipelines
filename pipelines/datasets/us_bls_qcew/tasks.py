"""Prefect 3 tasks for us_bls_qcew — thin wrappers over utils.py.

Two tasks split the work so the source poll runs before the expensive full
history clean: :func:`latest_source_period` cheaply probes BLS and returns the
newest NAICS quarterly period, and :func:`clean_qcew` re-cleans the whole NAICS
history only once the flow's poll guard confirms a new quarter (or a forced run).
"""

from pathlib import Path

from prefect import task

from pipelines.datasets.us_bls_qcew.constants import constants
from pipelines.datasets.us_bls_qcew.utils import (
    clean_naics_full_history,
    source_max_year_month,
)


@task(retries=2, retry_delay_seconds=30)
def latest_source_period(work_dir: str) -> str:
    """Probe BLS and return the newest NAICS quarterly period as ``"YYYY-MM"``.

    Downloads only the latest year's NAICS quarterly singlefile and scans its
    ``qtr`` column, so the poll decision costs one file rather than the full
    history. The quarter is mapped to its end month (``quarter * 3``), matching
    the coverage date the metadata layer reads from BigQuery.

    Retries twice: ``data.bls.gov`` intermittently drops the larger transfers.

    Args:
        work_dir: Directory to download into; files land under ``<work_dir>/input``.

    Returns:
        The source's max coverage period as ``"YYYY-MM"``.
    """
    input_dir = Path(work_dir) / "input"
    return source_max_year_month(
        input_dir, floor=constants.NAICS_START_YEAR.value
    )


@task
def clean_qcew(work_dir: str) -> dict:
    """Rebuild the 8 partitioned NAICS tables from the full published history.

    SIC is frozen and excluded; the dicionario is rebuilt but not returned as an
    upload target. Each singlefile is streamed in row chunks and pruned once
    cleaned, so peak memory is one chunk and disk holds only a few CSVs at a time.

    Args:
        work_dir: Directory to write into; tables land under ``<work_dir>/output``.

    Returns:
        A mapping of each NAICS table slug to its partitioned output directory.
    """
    input_dir = Path(work_dir) / "input"
    output_dir = Path(work_dir) / "output"
    clean_naics_full_history(
        input_dir,
        output_dir,
        floor=constants.NAICS_START_YEAR.value,
        download_workers=constants.PIPELINE_DOWNLOAD_WORKERS.value,
    )
    return {
        table: str(output_dir / table)
        for table in constants.NAICS_TABLES.value
    }
