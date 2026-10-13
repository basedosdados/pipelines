"""Prefect 3 tasks for us_treasury_fiscaldata — thin wrappers over utils.py."""

from pathlib import Path

from prefect import task

from pipelines.datasets.us_treasury_fiscaldata.utils import (
    clean_table,
    download_table,
)


@task(retries=2, retry_delay_seconds=30)
def download_fiscaldata(work_dir: str, table: str) -> str:
    """Download one table's full history from the FiscalData API.

    Retries twice: the paginated API occasionally drops a page mid-fetch.

    Args:
        work_dir: Directory to download into; files land in ``<work_dir>/input``.
        table: Data-table slug.

    Returns:
        The input directory path (Prefect serializes task results as strings).
    """
    input_dir = Path(work_dir) / "input"
    download_table(table, input_dir)
    return str(input_dir)


@task
def clean_fiscaldata(work_dir: str, input_dir: str, table: str) -> dict:
    """Clean one table into partitioned parquet.

    Args:
        work_dir: Directory to write into; output lands under ``<work_dir>/output``.
        input_dir: Directory holding the cached raw JSON, from
            :func:`download_fiscaldata`.
        table: Data-table slug.

    Returns:
        ``{"path": <str>, "max_date": <str|None>}`` — ``max_date`` drives the poll.
    """
    output_dir = Path(work_dir) / "output"
    res = clean_table(table, Path(input_dir), output_dir)
    return {"path": str(res["path"]), "max_date": res["max_date"]}
