"""Prefect 3 tasks for au_dcceew_greenhouse — thin wrappers over utils.py."""

from pathlib import Path

from prefect import task

from pipelines.datasets.au_dcceew_greenhouse.utils import (
    clean_all,
    download_all,
)


@task(retries=2, retry_delay_seconds=30)
def download_inventory(work_dir: str) -> str:
    """Download all 30 ANGA OData entity sets into ``<work_dir>/input``.

    Retries twice: the OData host occasionally drops the larger sets mid-stream.

    Returns:
        The input directory path (Prefect serializes task results).
    """
    input_dir = Path(work_dir) / "input"
    download_all(input_dir)
    return str(input_dir)


@task
def clean_inventory(work_dir: str, input_dir: str) -> dict:
    """Build the three partitioned tables from the downloaded JSON.

    Returns:
        A mapping of table slug to its partitioned output directory, plus
        ``"max_year"`` — the latest ``InventoryYear``, which drives the
        source-update poll.
    """
    output_dir = Path(work_dir) / "output"
    result = clean_all(Path(input_dir), output_dir)
    return {
        k: (str(v) if isinstance(v, Path) else v) for k, v in result.items()
    }
