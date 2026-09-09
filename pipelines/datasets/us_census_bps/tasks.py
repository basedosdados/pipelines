"""Prefect 3 tasks for us_census_bps — thin wrappers over utils.py."""

import time
from pathlib import Path

import basedosdados as bd
from prefect import task

from pipelines.datasets.us_census_bps.constants import constants
from pipelines.datasets.us_census_bps.utils import (
    clean_all,
    download_all,
    latest_monthly_period,
)


@task(retries=2, retry_delay_seconds=60)
def download_bps(work_dir: str) -> str:
    """Download every published Building Permits Survey file.

    Retries: www2.census.gov drops connections under sustained concurrency,
    and ``download_all`` raises rather than returning a partial set, so a
    retry is the difference between a complete table and one with months
    silently missing.

    Args:
        work_dir: Directory to download into; files land in ``<work_dir>/input``.

    Returns:
        The input directory path, as a string.
    """
    input_dir = Path(work_dir) / "input"
    download_all(input_dir, through_year=time.gmtime().tm_year)
    return str(input_dir)


@task
def clean_bps(work_dir: str, input_dir: str) -> dict:
    """Rebuild every table from the downloaded files.

    Args:
        work_dir: Directory to write into; tables land under ``<work_dir>/output``.
        input_dir: Directory holding the downloaded files.

    Returns:
        A mapping of table slug to its partitioned output directory, plus
        ``"max_year_month"``, the latest ``YYYY-MM`` the source has published,
        which drives the source poll.
    """
    output = Path(work_dir) / "output"
    counts = clean_all(Path(input_dir), output)
    result: dict = {
        table: str(output / table) for table in constants.TABLES.value
    }
    result["row_counts"] = counts
    result["max_year_month"] = latest_monthly_period(Path(input_dir))
    return result


@task(retries=2, retry_delay_seconds=30)
def clear_staging_prefix(table_id: str, bucket_name: str) -> None:
    """Delete a table's staging objects before a full rebuild uploads new ones.

    Every run rebuilds the whole series, so the previous run's objects have to
    go. ``dump_mode="overwrite"`` would do it, but it also calls
    ``tb.delete(mode="all")``, which drops the **materialized production
    table** — and it fires from the dev half too, because ``bd.Table`` resolves
    its BigQuery projects from the pod config rather than from ``bucket_name``.
    A dev-only smoke run has silently deleted a production table this way
    before.

    ``dump_mode="append"`` avoids that, but it uploads object by object and
    replaces only the names it writes. A run that produces fewer part files
    than the last one would leave the extra ones behind, and the external table
    would read them as real rows. Clearing the prefix first closes that gap
    without touching any BigQuery table.

    Args:
        table_id: Table slug.
        bucket_name: Bucket holding the staging prefix.
    """
    storage = bd.Storage(
        dataset_id=constants.DATASET_ID.value,
        table_id=table_id,
        bucket_name=bucket_name,
        billing_project_id=bucket_name,
    )
    storage.delete_table(
        mode="staging", bucket_name=bucket_name, not_found_ok=True
    )
    print(
        f"cleared gs://{bucket_name}/staging/"
        f"{constants.DATASET_ID.value}/{table_id}"
    )
