"""Prefect 3 tasks for us_fhfa_hpi — thin wrappers over utils.py."""

from pathlib import Path

from prefect import task

from pipelines.datasets.us_fhfa_hpi.utils import (
    clean_annual,
    clean_master,
    download_annual,
    download_master,
)


@task(retries=2, retry_delay_seconds=30)
def download_master_task(work_dir: str) -> str:
    """Download ``hpi_master.csv``.

    Args:
        work_dir: Directory to download into; the file lands in ``<work_dir>/input``.

    Returns:
        The input directory path, as a string (Prefect serializes task results).
    """
    input_dir = Path(work_dir) / "input"
    download_master(input_dir)
    return str(input_dir)


@task(retries=2, retry_delay_seconds=30)
def download_annual_task(work_dir: str) -> str:
    """Download the seven annual developmental index files.

    Args:
        work_dir: Directory to download into; files land in ``<work_dir>/input``.

    Returns:
        The input directory path, as a string.
    """
    input_dir = Path(work_dir) / "input"
    download_annual(input_dir)
    return str(input_dir)


@task
def clean_master_task(work_dir: str, input_dir: str) -> dict:
    """Build the four master tables plus the dictionary.

    Args:
        work_dir: Directory to write into; tables land under ``<work_dir>/output``.
        input_dir: Directory holding ``hpi_master.csv``.

    Returns:
        ``{"paths": ..., "counts": ..., "max_year_month": "YYYY-MM"}``.
    """
    return clean_master(Path(input_dir), Path(work_dir) / "output")


@task
def clean_annual_task(work_dir: str, input_dir: str) -> dict:
    """Build the seven annual tables.

    Args:
        work_dir: Directory to write into; tables land under ``<work_dir>/output``.
        input_dir: Directory holding the downloaded annual files.

    Returns:
        ``{"paths": ..., "counts": ..., "max_year": "YYYY"}``.
    """
    return clean_annual(Path(input_dir), Path(work_dir) / "output")
