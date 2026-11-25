"""Prefect 3 tasks for us_cfpb_complaints — thin wrappers over utils.py."""

from pathlib import Path

from prefect import task

from pipelines.datasets.us_cfpb_complaints.utils import (
    clean_all,
    download_snapshot,
)


@task(retries=2, retry_delay_seconds=60)
def download_complaints(work_dir: str) -> str:
    """Download and unzip the CFPB full-database export.

    Retries twice: the export is ~1.4 GB over a single connection and
    files.consumerfinance.gov intermittently drops it — a broken pipe part-way
    through was observed during the initial load.

    Args:
        work_dir: Directory to download into; files land in ``<work_dir>/input``.

    Returns:
        The input directory path, as a string (Prefect serializes task results).
    """
    input_dir = Path(work_dir) / "input"
    download_snapshot(input_dir)
    return str(input_dir)


@task
def clean_complaints(work_dir: str, input_dir: str) -> dict:
    """Build both tables from the downloaded snapshot.

    Args:
        work_dir: Directory to write into; tables land under ``<work_dir>/output``.
        input_dir: Directory holding the unzipped CSV, from :func:`download_complaints`.

    Returns:
        Table slug -> partitioned output directory (as strings), plus
        ``"max_date_received"`` — the latest ``"YYYY-MM-DD"`` in the snapshot,
        which drives the source-update poll — and ``"row_counts"``.
    """
    output_dir = Path(work_dir) / "output"
    result = clean_all(Path(input_dir), output_dir)
    return {
        k: (str(v) if isinstance(v, Path) else v) for k, v in result.items()
    }
