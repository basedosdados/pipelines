"""Prefect 3 tasks for mx_sesnsp_incidencia_delictiva — thin wrappers over utils.py."""

from pathlib import Path

from prefect import task

from pipelines.datasets.mx_sesnsp_incidencia_delictiva.utils import (
    clean_all,
    download_all,
)


@task(retries=2, retry_delay_seconds=60)
def download_sesnsp(work_dir: str) -> str:
    """Scrape the current SharePoint tokens and download the four ongoing tables.

    Retries twice: the Imperva challenge and the SharePoint download both drop
    connections intermittently.

    Args:
        work_dir: Directory to download into; files land in ``<work_dir>/input``.

    Returns:
        The input directory path, as a string (Prefect serializes task results).
    """
    input_dir = Path(work_dir) / "input"
    download_all(input_dir)
    return str(input_dir)


@task
def clean_sesnsp(work_dir: str, input_dir: str) -> dict:
    """Build the four partitioned tables from the downloaded CSVs.

    Args:
        work_dir: Directory to write into; tables land under ``<work_dir>/output``.
        input_dir: Directory holding the downloaded CSVs, from
            :func:`download_sesnsp`.

    Returns:
        A mapping of table slug to its partitioned output directory, plus
        ``"max_year_month"`` — the latest ``"YYYY-MM"`` present across the four
        tables, which drives the source-update poll.
    """
    output_dir = Path(work_dir) / "output"
    result = clean_all(Path(input_dir), output_dir)
    return {
        k: (str(v) if isinstance(v, Path) else v) for k, v in result.items()
    }
