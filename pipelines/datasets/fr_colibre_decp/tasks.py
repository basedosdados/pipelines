"""Prefect task wrappers over the pure helpers in utils.py."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import basedosdados as bd
from prefect import task

from pipelines.datasets.fr_colibre_decp import utils
from pipelines.datasets.fr_colibre_decp.constants import constants
from pipelines.utils.tasks import _upload_to_gcs


def replace_staging(table_dir: Path, table: str, bucket_name: str) -> None:
    """Replace a table's staging files with ``table_dir``, keeping its tables.

    The source is rebuilt from scratch every day, so each run replaces the whole
    staging prefix. Deleting the prefix first means a partition that vanished at
    source cannot survive as a stale file. ``dump_mode="overwrite"`` would also do
    that, but it calls ``Table.delete(mode="all")``, which drops the materialised
    table too and leaves it missing until dbt rebuilds it, or for good if dbt
    fails. Clearing only the GCS prefix and then appending keeps the existing
    staging and materialised tables in place.
    """
    bd.Storage(
        dataset_id=constants.DATASET_ID.value,
        table_id=table,
        bucket_name=bucket_name,
        billing_project_id=bucket_name,
    ).delete_table(mode="staging", bucket_name=bucket_name, not_found_ok=True)
    _upload_to_gcs(
        data_path=str(table_dir),
        dataset_id=constants.DATASET_ID.value,
        table_id=table,
        bucket_name=bucket_name,
        dump_mode="append",
        source_format="parquet",
    )


@task(retries=2, retry_delay_seconds=60)
def source_last_modified_task() -> datetime:
    return utils.source_last_modified()


@task(retries=2, retry_delay_seconds=120)
def download_and_clean_task(root: str) -> str:
    """Download decp.parquet and write the three tables under ``<root>/output``."""
    source = utils.download_decp(Path(root) / "input")
    counts = utils.clean_decp(source, Path(root) / "output")
    print(f"cleaned: {counts}")
    return str(Path(root) / "output")


@task
def source_max_date_task(output_dir: str) -> str:
    return utils.source_max_date(Path(output_dir))


@task(retries=3, retry_delay_seconds=30)
def replace_staging_task(
    output_dir: str, table: str, bucket_name: str
) -> None:
    replace_staging(Path(output_dir) / table, table, bucket_name)
