"""Prefect 3 tasks for us_ucla_voteview — thin wrappers over utils.py."""

from pathlib import Path

from prefect import task

from pipelines.datasets.us_ucla_voteview.utils import clean_all, download_all


@task(retries=2, retry_delay_seconds=60)
def download_voteview(work_dir: str) -> str:
    """Download the four Voteview ``HSall_*.csv`` files (about 740 MB in total).

    Args:
        work_dir: Directory to download into; files land in ``<work_dir>/input``.

    Returns:
        The input directory path, as a string.
    """
    input_dir = Path(work_dir) / "input"
    download_all(input_dir)
    return str(input_dir)


@task
def clean_voteview(work_dir: str, input_dir: str) -> dict:
    """Build the five tables as all-STRING parquet under ``<work_dir>/output``.

    Returns:
        ``{table: output directory}`` for every table, plus ``"max_date"``, the
        latest roll call date (``YYYY-MM-DD``), which drives the source poll.
    """
    output_dir = Path(work_dir) / "output"
    result = clean_all(Path(input_dir), output_dir)
    paths = {t: str(output_dir / t) for t in result["rows"]}
    return {**paths, "max_date": result["max_date"]}
