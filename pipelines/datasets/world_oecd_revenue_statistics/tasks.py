"""Prefect 3 tasks for world_oecd_revenue_statistics — thin wrappers over utils.py."""

from pathlib import Path

from prefect import task

from pipelines.datasets.world_oecd_revenue_statistics.utils import (
    clean_all,
    download_all,
)


@task(retries=1, retry_delay_seconds=120)
def download_revenue(work_dir: str) -> str:
    """Download the OECD comparative cube into ``<work_dir>/input``.

    Resume-safe and adaptively throttled. The OECD host Cloudflare-challenges
    heavy automated pulls, so this can fail on a worker; a pre-staged
    ``<work_dir>/input`` (CSVs already present) is reused as-is.

    Returns the input directory path.
    """
    input_dir = Path(work_dir) / "input"
    download_all(input_dir)
    return str(input_dir)


@task
def clean_revenue(work_dir: str, input_dir: str) -> dict:
    """Clean the downloaded CSVs into partitioned parquet under ``<work_dir>/output``.

    Returns a mapping of table slug to its output directory, plus ``max_year``
    (the latest year present, which drives the source-update poll).
    """
    output_dir = Path(work_dir) / "output"
    structure_cache = Path(input_dir) / "structure" / "dsd.xml"
    result = clean_all(Path(input_dir), output_dir, structure_cache)
    return {
        k: (str(v) if isinstance(v, Path) else v) for k, v in result.items()
    }
