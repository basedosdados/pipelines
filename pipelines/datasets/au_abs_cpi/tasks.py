"""Prefect 3 tasks for au_abs_cpi — thin wrappers over utils.py."""

from pathlib import Path

from prefect import task

from pipelines.datasets.au_abs_cpi.utils import clean_all, download_all


@task(retries=2, retry_delay_seconds=30)
def download_cpi(work_dir: str) -> str:
    """Download the current ABS CPI release into ``<work_dir>/input``.

    Retries twice: www.abs.gov.au intermittently drops the larger by-city file.
    """
    input_dir = Path(work_dir) / "input"
    input_dir.mkdir(parents=True, exist_ok=True)
    download_all(str(input_dir))
    return str(input_dir)


@task
def clean_cpi(work_dir: str, input_dir: str) -> dict:
    """Build the quarterly and monthly tables under ``<work_dir>/output``.

    Returns the per-table partition roots plus ``"max_year_month"`` (the latest
    ``"YYYY-MM"`` in the monthly table), which drives the source-update poll.
    """
    output_dir = Path(work_dir) / "output"
    return clean_all(str(input_dir), str(output_dir))
