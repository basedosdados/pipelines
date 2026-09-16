"""Prefect 3 tasks for us_bls_cpi — thin wrappers over utils.py."""

from pathlib import Path

from prefect import task

from pipelines.datasets.us_bls_cpi.utils import clean_all, download_flatfiles


@task(retries=2, retry_delay_seconds=30)
def download_cpi(work_dir: str) -> str:
    """Download the CPI-U and CPI-W flat files from BLS.

    Retries twice: download.bls.gov intermittently rejects or drops connections
    on the larger by-group files.

    Args:
        work_dir: Directory to download into; files land in ``<work_dir>/input``.

    Returns:
        The input directory path, as a string (Prefect serializes task results).
    """
    input_dir = Path(work_dir) / "input"
    download_flatfiles(input_dir)
    return str(input_dir)


@task
def clean_cpi(work_dir: str, input_dir: str) -> dict:
    """Build the four partitioned tables from the downloaded flat files.

    Args:
        work_dir: Directory to write into; tables land under ``<work_dir>/output``.
        input_dir: Directory holding the downloaded flat files, from
            :func:`download_cpi`.

    Returns:
        A mapping of table slug to its partitioned output directory, plus
        ``"max_year_month"`` — the latest ``"YYYY-MM"`` present in the monthly
        table, which drives the source-update poll.
    """
    output_dir = Path(work_dir) / "output"
    result = clean_all(Path(input_dir), output_dir)
    return {
        k: (str(v) if isinstance(v, Path) else v) for k, v in result.items()
    }
