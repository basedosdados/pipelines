"""Prefect 3 tasks for us_bls_cpi — thin wrappers over utils.py."""

from pathlib import Path

from prefect import task

from pipelines.datasets.us_bls_cpi.utils import clean_all, download_flatfiles


@task(retries=2, retry_delay_seconds=30)
def download_cpi(work_dir: str) -> str:
    """Download CPI-U + CPI-W flat files into <work_dir>/input; return that path."""
    input_dir = Path(work_dir) / "input"
    download_flatfiles(input_dir)
    return str(input_dir)


@task
def clean_cpi(work_dir: str, input_dir: str) -> dict:
    """Build the four partitioned tables into <work_dir>/output. Returns
    {table: output_dir_str, "max_year_month": "YYYY-MM"}."""
    output_dir = Path(work_dir) / "output"
    result = clean_all(Path(input_dir), output_dir)
    return {
        k: (str(v) if isinstance(v, Path) else v) for k, v in result.items()
    }
