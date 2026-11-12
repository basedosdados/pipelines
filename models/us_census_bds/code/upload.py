"""Upload the cleaned us_census_bds Parquet to BigQuery staging in dev.

Uploads each table's partition files to GCS, then defines the staging table
as an all-STRING EXTERNAL Parquet table over the prefix, hive-partitioned on
``year``. All-STRING matches what the recurring pipeline's ``upload_to_gcs``
writes, so the two never collide.

Row counts are asserted against ``output/manifest.json``, written by the
cleaning step.

Usage:
    ~/.venvs/bd-pipelines/bin/python -m models.us_census_bds.code.upload [table ...]
"""

from __future__ import annotations

import argparse
import json
import tomllib
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pyarrow.parquet as pq
from google.cloud import bigquery, storage
from google.oauth2 import service_account

from pipelines.datasets.us_census_bds.constants import constants

DATASET_ID = constants.DATASET_ID.value
PROJECT = "basedosdados-dev"
BUCKET = "basedosdados-dev"
OUTPUT = constants.DATA_DIR.value / "output"
TABLES = ["dictionary", "country", "state", "county", "metropolitan_area"]


def clients() -> tuple[storage.Client, bigquery.Client]:
    """Build GCS and BigQuery clients from the basedosdados service account."""
    config = tomllib.loads(
        (Path.home() / ".basedosdados/config.toml").read_text()
    )
    path = config["gcloud-projects"]["staging"]["credentials_path"]
    creds = service_account.Credentials.from_service_account_file(path)
    return (
        storage.Client(credentials=creds, project=PROJECT),
        bigquery.Client(credentials=creds, project=PROJECT),
    )


def expected_rows(table: str) -> int:
    """Return the row count the cleaning step wrote for a table."""
    if table == "dictionary":
        return pq.read_metadata(OUTPUT / table / "data.parquet").num_rows
    manifest = json.loads((OUTPUT / "manifest.json").read_text())
    return sum(e["rows"] for e in manifest.values() if e["table"] == table)


def upload_table(gcs: storage.Client, bq: bigquery.Client, table: str) -> int:
    """Replace one table's staging files and redefine its external table."""
    local = OUTPUT / table
    files = sorted(local.rglob("*.parquet"))
    if not files:
        raise FileNotFoundError(local)
    prefix = f"staging/{DATASET_ID}/{table}/"
    # The bucket is requester-pays, so every call needs a billing project.
    bucket = gcs.bucket(BUCKET, user_project=PROJECT)

    stale = list(bucket.list_blobs(prefix=prefix))
    for blob in stale:
        blob.delete()
    print(f"  cleared {len(stale)} stale objects under {prefix}")

    def put(path: Path) -> None:
        blob = bucket.blob(prefix + str(path.relative_to(local)))
        blob.upload_from_filename(str(path))

    with ThreadPoolExecutor(16) as pool:
        list(pool.map(put, files))
    print(f"  uploaded {len(files)} files")

    ref = f"{PROJECT}.{DATASET_ID}_staging.{table}"
    bq.query(f"drop table if exists `{ref}`").result()
    config = bigquery.ExternalConfig("PARQUET")
    config.source_uris = [f"gs://{BUCKET}/{prefix}*"]
    if table != "dictionary":
        hive = bigquery.HivePartitioningOptions()
        hive.mode = "STRINGS"
        hive.source_uri_prefix = f"gs://{BUCKET}/{prefix}"
        config.hive_partitioning = hive
    definition = bigquery.Table(ref)
    definition.external_data_configuration = config
    bq.create_table(definition)

    expected = expected_rows(table)
    row = next(iter(bq.query(f"select count(*) as n from `{ref}`").result()))
    if row.n != expected:
        raise ValueError(
            f"{table}: loaded {row.n:,} rows, expected {expected:,}"
        )
    print(f"  {table}: {row.n:,} rows — OK")
    return int(row.n)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("tables", nargs="*")
    args = parser.parse_args()
    gcs, bq = clients()
    bq.create_dataset(f"{PROJECT}.{DATASET_ID}_staging", exists_ok=True)
    for table in [t for t in TABLES if not args.tables or t in args.tables]:
        print(f"=== {table} ===", flush=True)
        upload_table(gcs, bq, table)
    print("ALL TABLES UPLOADED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
