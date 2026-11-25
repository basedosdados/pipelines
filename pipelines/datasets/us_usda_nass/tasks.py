"""Prefect 3 tasks for us_usda_nass — thin wrappers over utils.py."""

from pathlib import Path

from prefect import task

from pipelines.datasets.us_usda_nass.utils import clean_all, download_bulk


@task(retries=2, retry_delay_seconds=60)
def download_nass(work_dir: str) -> str:
    """Download the 5 QuickStats bulk sector files.

    Retries twice: the sector files are large and the transfer can drop.

    Args:
        work_dir: Directory to download into; files land in ``<work_dir>/input``.

    Returns:
        The input directory path, as a string.
    """
    input_dir = Path(work_dir) / "input"
    download_bulk(input_dir)
    return str(input_dir)


@task
def clean_nass(work_dir: str, input_dir: str) -> dict:
    """Stream, filter and write the two fact tables plus the dicionario.

    Args:
        work_dir: Directory to write into; tables land under ``<work_dir>/output``.
        input_dir: Directory holding the downloaded ``.gz`` files.

    Returns:
        The :func:`clean_all` result: table slug -> output dir (str), plus
        ``"max_year"`` (int) and ``"counts"``.
    """
    output_dir = Path(work_dir) / "output"
    return clean_all(Path(input_dir), output_dir)
