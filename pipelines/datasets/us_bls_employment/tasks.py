"""Prefect 3 tasks for us_bls_employment — thin wrappers over utils.py."""

from pathlib import Path

from prefect import task

from pipelines.datasets.us_bls_employment.utils import (
    clean_all,
    download_flatfiles,
)


@task(retries=2, retry_delay_seconds=30)
def download_employment(work_dir: str) -> str:
    """Download the CES, SAE, LAUS and JOLTS flat files from BLS.

    Retries twice: download.bls.gov intermittently drops connections on the
    larger per-state files, and a dropped connection would otherwise look like a
    successful short file. Every file is size-checked inside
    :func:`~pipelines.datasets.us_bls_employment.utils.download_flatfiles`.

    Args:
        work_dir: Directory to download into; files land in ``<work_dir>/input``.

    Returns:
        The input directory path, as a string (Prefect serializes task results).
    """
    input_dir = Path(work_dir) / "input"
    download_flatfiles(input_dir)
    return str(input_dir)


@task
def clean_employment(work_dir: str, input_dir: str) -> dict:
    """Build the four fact tables and the dictionary from the flat files.

    Args:
        work_dir: Directory to write into; tables land under
            ``<work_dir>/output``.
        input_dir: Directory holding the downloaded flat files, from
            :func:`download_employment`.

    Returns:
        A mapping of table slug to its partitioned output directory, plus
        ``"max_year_month"`` — the latest ``"YYYY-MM"`` across the programs,
        which drives the source-update poll.
    """
    output_dir = Path(work_dir) / "output"
    result = clean_all(Path(input_dir), output_dir)
    out: dict = {}
    for table, summary in result.items():
        if table == "max_year_month":
            out[table] = summary
        elif table == "dicionario":
            out[table] = str(output_dir / "dicionario")
        else:
            out[table] = str(output_dir / table)
            out[f"{table}_summary"] = summary
    return out
