"""Prefect 3 tasks for us_bls_cex — thin wrappers over utils.py."""

from pathlib import Path

from prefect import task

from pipelines.datasets.us_bls_cex.utils import (
    clean_labstat,
    download_labstat,
    latest_published_year,
)


@task(retries=2, retry_delay_seconds=60)
def get_latest_year() -> str:
    """Latest published year in the cx database, as ``"YYYY"``.

    Retries: api.bls.gov occasionally times out.
    """
    return str(latest_published_year())


@task(retries=2, retry_delay_seconds=30)
def download_cex(work_dir: str) -> str:
    """Download the cx LABSTAT flat files from BLS.

    Retries twice: download.bls.gov intermittently drops connections on the
    ~740 MB cx.aspect file.

    Args:
        work_dir: Directory to download into; files land in ``<work_dir>/input``.

    Returns:
        The input directory path, as a string (Prefect serializes task results).
    """
    input_dir = Path(work_dir) / "input"
    download_labstat(input_dir)
    return str(input_dir)


@task
def clean_cex(work_dir: str, input_dir: str) -> dict:
    """Build the ``series`` and ``annual`` tables from the downloaded files.

    Args:
        work_dir: Directory to write into; tables land under ``<work_dir>/output``.
        input_dir: Directory holding the flat files, from :func:`download_cex`.

    Returns:
        Mapping of table slug to its output directory, plus ``"max_year"`` —
        the latest year in ``annual``.
    """
    result = clean_labstat(Path(input_dir), Path(work_dir) / "output")
    return {
        k: (str(v) if isinstance(v, Path) else v) for k, v in result.items()
    }
