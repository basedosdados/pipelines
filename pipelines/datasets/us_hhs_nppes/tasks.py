"""Prefect 3 tasks for us_hhs_nppes — thin wrappers over utils.py."""

from pathlib import Path

from prefect import task

from pipelines.datasets.us_hhs_nppes.utils import (
    clean_all,
    download_monthly,
    source_last_modified,
)


@task(retries=2, retry_delay_seconds=30)
def check_source_nppes() -> str:
    """Return the monthly bundle's publication date ("YYYY-MM-DD") via HTTP HEAD.

    Cheap poll signal — no data download. Compared against ``Table.Update.latest``
    so the flow only fetches the ~1.1 GB payload once CMS has republished.
    """
    return source_last_modified()


@task(retries=2, retry_delay_seconds=120)
def download_nppes(work_dir: str) -> str:
    """Download the monthly full-replacement ZIP into ``<work_dir>/input``.

    Returns:
        The input directory path (Prefect serializes task results).
    """
    input_dir = Path(work_dir) / "input"
    download_monthly(input_dir)
    return str(input_dir)


@task
def clean_nppes(work_dir: str, input_dir: str) -> dict:
    """Clean the bundle into partitioned parquet under ``<work_dir>/output``.

    Returns:
        A mapping of table slug to its partitioned output directory, plus
        ``"max_extraction_date"`` (the snapshot's reference date) and
        ``"counts"``.
    """
    output_dir = Path(work_dir) / "output"
    result = clean_all(Path(input_dir), output_dir)
    return {
        k: (str(v) if isinstance(v, Path) else v) for k, v in result.items()
    }
