"""Prefect 3 tasks for us_congress_legislators — thin wrappers over utils.py."""

from pathlib import Path

from prefect import task

from pipelines.datasets.us_congress_legislators.utils import (
    clean_all,
    download_sources,
    latest_source_commit_date,
)


@task(retries=2, retry_delay_seconds=60)
def get_source_date() -> str:
    """Date of the latest commit to the source repo, as ``"YYYY-MM-DD"``."""
    return latest_source_commit_date().isoformat()


@task(retries=2, retry_delay_seconds=30)
def download_legislators(work_dir: str) -> str:
    """Download every source YAML file into ``<work_dir>/input``.

    Returns:
        The input directory path, as a string (Prefect serializes task results).
    """
    input_dir = Path(work_dir) / "input"
    download_sources(input_dir)
    return str(input_dir)


@task
def clean_legislators(work_dir: str, input_dir: str) -> dict[str, str]:
    """Build every table under ``<work_dir>/output``.

    Returns:
        A mapping of ``"<dataset_id>.<table_id>"`` to the table's output
        directory, in upload order.
    """
    return clean_all(Path(input_dir), Path(work_dir) / "output")
