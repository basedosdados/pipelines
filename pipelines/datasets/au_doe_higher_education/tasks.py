"""Prefect tasks for the au_doe_higher_education annual refresh."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
from prefect import task

from pipelines.datasets.au_doe_higher_education.constants import constants
from pipelines.datasets.au_doe_higher_education.utils import (
    build_all,
    discover_sources,
    download_sources,
    merge_institution_directory,
    observed_institutions,
    refreshed_partitions,
    source_max_year,
    write_partitioned,
)
from pipelines.utils.gcs import get_credentials_from_env


@task(retries=3, retry_delay_seconds=[60, 300, 900])
def discover_sources_task() -> dict[str, dict]:
    """Locate the newest release of every source document."""
    sources = discover_sources()
    for name, entry in sorted(sources.items()):
        print(f"{name:20} {entry['year']}  {entry['slug']}")
    return sources


@task
def source_max_year_task(sources: dict[str, dict]) -> str:
    """The newest reference year across the source, as ``YYYY``."""
    return str(source_max_year(sources))


@task(retries=3, retry_delay_seconds=[60, 300, 900])
def download_task(input_dir: str, sources: dict[str, dict]) -> str:
    """Download every discovered document into ``input_dir``."""
    download_sources(input_dir, sources)
    return input_dir


@task
def clean_task(input_dir: str, output_dir: str) -> dict[str, list[str]]:
    """Build every table and write partitioned Parquet.

    Returns the partition values each table covers, so the upload step can
    replace exactly those and leave older partitions untouched.
    """
    built = build_all(input_dir)
    override = constants.PARTITION_OVERRIDE.value

    for table, frame in built.items():
        rows = write_partitioned(
            frame, output_dir, table, override.get(table, "year")
        )
        print(f"{table:36} {rows:>8,} rows")

    directory = merge_institution_directory(
        read_published_directory(), observed_institutions(built)
    )
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    directory.to_parquet(
        Path(output_dir) / "higher_education_institution.parquet"
    )
    print(f"directory {len(directory):>8,} rows")

    return refreshed_partitions(built, override)


def read_published_directory() -> pd.DataFrame:
    """The institution directory as currently published."""
    from google.cloud import bigquery

    credentials = get_credentials_from_env(mode="prod")
    client = bigquery.Client(
        credentials=credentials, project=credentials.project_id
    )
    return client.query(
        "select * from "
        f"`basedosdados.{constants.DIRECTORY_DATASET_ID.value}"
        f".{constants.DIRECTORY_TABLE_ID.value}`"
    ).to_dataframe()


@task
def delete_staging_partitions_task(
    covered: dict[str, list[str]], bucket_name: str
) -> None:
    """Delete only the staging partitions the run rebuilt.

    ``upload_to_gcs`` appends, so re-uploading a partition that is already
    there would double every row in it. Deleting the exact prefixes first
    makes the upload a replace for those partitions and a no-op for the rest.
    """
    from google.cloud import storage

    credentials = get_credentials_from_env(
        mode="prod" if bucket_name == "basedosdados" else "staging"
    )
    bucket = storage.Client(
        credentials=credentials, project=bucket_name
    ).bucket(bucket_name)
    override = constants.PARTITION_OVERRIDE.value
    dataset = constants.DATASET_ID.value

    for table, values in covered.items():
        column = override.get(table, "year")
        for value in values:
            prefix = f"staging/{dataset}/{table}/{column}={value}/"
            blobs = list(bucket.list_blobs(prefix=prefix))
            for blob in blobs:
                blob.delete()
            if blobs:
                print(f"cleared {prefix} ({len(blobs)} object(s))")
