"""Prefect 3 tasks for us_epa_ghgrp — thin wrappers over utils.py."""

from pathlib import Path

from prefect import task

from pipelines.datasets.us_epa_ghgrp.utils import (
    clean_all,
    download_all,
    source_max_year,
)


@task(retries=2, retry_delay_seconds=30)
def source_max_year_task() -> str:
    """Latest reporting year the API holds, as ``"YYYY"``.

    A handful of count requests, so the scheduled run can decide whether there
    is anything new before downloading half a million rows.
    """
    return source_max_year()


@task(retries=2, retry_delay_seconds=60)
def download_task(work_dir: str) -> str:
    """Download the GHG API tables.

    Args:
        work_dir: Directory to download into; files land in ``<work_dir>/input/api``.

    Returns:
        The input directory path, as a string (Prefect serializes task results).
    """
    input_dir = Path(work_dir) / "input"
    download_all(input_dir)
    return str(input_dir)


@task
def clean_task(work_dir: str, input_dir: str) -> dict:
    """Build the three data tables plus the dictionary.

    Args:
        work_dir: Directory to write into; tables land under ``<work_dir>/output``.
        input_dir: Directory holding the downloaded ``api/`` CSVs.

    Returns:
        ``{"paths": ..., "counts": ..., "max_year": "YYYY"}``.
    """
    return clean_all(Path(input_dir), Path(work_dir) / "output")
