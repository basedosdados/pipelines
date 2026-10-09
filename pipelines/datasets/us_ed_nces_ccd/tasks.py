"""Prefect 3 tasks for us_ed_nces_ccd — thin wrappers over utils.py."""

from pathlib import Path

from prefect import task

from pipelines.datasets.us_ed_nces_ccd.utils import (
    clean_year,
    download_directories,
    rebuild_dictionary,
    source_max_year,
)


@task(retries=3, retry_delay_seconds=60)
def download_ccd(work_dir: str) -> str:
    """Download the CCD school and agency directory extracts from the portal.

    Retries three times: the portal stalls large transfers often enough that a
    single attempt is not reliable.

    Args:
        work_dir: Directory to download into; files land in ``<work_dir>/input``.

    Returns:
        The input directory path, as a string (Prefect serializes task results).
    """
    input_dir = Path(work_dir) / "input"
    download_directories(input_dir)
    return str(input_dir)


@task
def latest_source_year(input_dir: str) -> int:
    """Latest school year present in the downloaded school directory extract."""
    return source_max_year(Path(input_dir))


@task(retries=2, retry_delay_seconds=60)
def clean_ccd(work_dir: str, input_dir: str, year: int) -> dict:
    """Build the four refreshable tables plus the dictionary for one school year.

    Args:
        work_dir: Directory to write into; tables land under ``<work_dir>/output``.
        input_dir: Directory holding the downloaded extracts, from :func:`download_ccd`.
        year: School year (fall) to build.

    Returns:
        A mapping of table slug to its partitioned output directory.
    """
    output_dir = Path(work_dir) / "output"
    result = clean_year(Path(input_dir), output_dir, year)
    result["dicionario"] = rebuild_dictionary(output_dir)
    return result
