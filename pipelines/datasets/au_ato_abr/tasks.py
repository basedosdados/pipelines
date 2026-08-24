"""Prefect 3 tasks for au_ato_abr — thin wrappers over utils.py."""

from pathlib import Path

from prefect import task

from pipelines.datasets.au_ato_abr.utils import (
    clean_all,
    download_zips,
    source_last_modified,
)


@task(retries=2, retry_delay_seconds=30)
def check_source_abr() -> str:
    """Return the source's newest publication date ("YYYY-MM-DD") via HTTP HEAD.

    Cheap poll signal — no data download. Compared against ``Table.Update.latest``
    so the flow only fetches the payload when the source has republished.
    """
    return source_last_modified()


@task(retries=2, retry_delay_seconds=60)
def download_abr(work_dir: str) -> str:
    """Download the two ABN Bulk Extract ZIPs into ``<work_dir>/input``.

    Returns:
        The input directory path (Prefect serializes task results).
    """
    input_dir = Path(work_dir) / "input"
    download_zips(input_dir)
    return str(input_dir)


@task
def clean_abr(work_dir: str, input_dir: str) -> dict:
    """Parse the ZIPs into partitioned parquet under ``<work_dir>/output``.

    Returns:
        A mapping of table slug to its partitioned output directory, plus
        ``"max_extraction_date"`` (the snapshot date) and ``"counts"``.
    """
    output_dir = Path(work_dir) / "output"
    result = clean_all(Path(input_dir), output_dir)
    return {
        k: (str(v) if isinstance(v, Path) else v) for k, v in result.items()
    }
