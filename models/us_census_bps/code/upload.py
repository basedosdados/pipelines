"""Upload the cleaned us_census_bps Parquet to BigQuery staging.

Streams each file to GCS and then defines the staging table as an EXTERNAL
Parquet table over the uploaded prefix. This reproduces exactly what
``basedosdados.Table.create`` produces — an all-STRING external table,
hive-partitioned on ``year`` — without its pandas step, which reads the whole
file into memory and would need tens of gigabytes for the 19-million-row place
table.

All-STRING matters beyond memory: the recurring pipeline's ``upload_to_gcs``
writes an all-STRING staging table over the same prefix, so a typed table left
here would collide with it on the first pipeline run.

Usage:
    uv run python models/us_census_bps/code/upload.py [table ...]
"""

from __future__ import annotations

import argparse
import os
import sys
import tomllib
from pathlib import Path

from google.cloud import bigquery, storage
from google.oauth2 import service_account

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from pipelines.datasets.us_census_bps.constants import constants

DATASET_ID = constants.DATASET_ID.value
PROJECT = "basedosdados-dev"
BUCKET = "basedosdados-dev"
DATA_DIR = Path(
    os.environ.get(
        "BPS_DATA_DIR", Path.home() / "Downloads/us_census_bps_data"
    )
)
OUTPUT = DATA_DIR / "output"


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


def upload_table(
    gcs: storage.Client, bq: bigquery.Client, table: str, expected: int
) -> int:
    """Stream one table's Parquet to GCS and redefine its staging table.

    Args:
        gcs: Storage client.
        bq: BigQuery client.
        table: Table slug.
        expected: Row count the cleaning step reported, asserted after load.

    Returns:
        The row count read back from BigQuery.

    Raises:
        FileNotFoundError: If the table has no local output.
        ValueError: If the loaded row count differs from ``expected``.
    """
    local = OUTPUT / table
    if not local.exists():
        raise FileNotFoundError(local)
    files = sorted(local.rglob("*.parquet"))
    prefix = f"staging/{DATASET_ID}/{table}/"
    # The bucket is requester-pays, so every call needs a billing project.
    bucket = gcs.bucket(BUCKET, user_project=PROJECT)

    stale = list(bucket.list_blobs(prefix=prefix))
    for blob in stale:
        blob.delete()
    print(f"  cleared {len(stale)} stale objects under {prefix}")

    for i, path in enumerate(files, start=1):
        blob = bucket.blob(prefix + str(path.relative_to(local)))
        blob.chunk_size = 64 * 1024 * 1024
        blob.upload_from_filename(str(path))
        if i % 25 == 0 or i == len(files):
            print(f"  uploaded {i}/{len(files)}", flush=True)

    ref = f"{PROJECT}.{DATASET_ID}_staging.{table}"
    bq.query(f"drop table if exists `{ref}`").result()
    config = bigquery.ExternalConfig("PARQUET")
    config.source_uris = [f"gs://{BUCKET}/{prefix}*"]
    if table != "dicionario":
        hive = bigquery.HivePartitioningOptions()
        hive.mode = "STRINGS"
        hive.source_uri_prefix = f"gs://{BUCKET}/{prefix}"
        config.hive_partitioning = hive
    definition = bigquery.Table(ref)
    definition.external_data_configuration = config
    bq.create_table(definition)

    row = next(iter(bq.query(f"select count(*) as n from `{ref}`").result()))
    if row.n != expected:
        raise ValueError(
            f"{table}: loaded {row.n:,} rows, expected {expected:,}"
        )
    print(f"  {table}: {row.n:,} rows — OK")
    return int(row.n)


# Row counts reported by models/us_census_bps/code/clean.py, smallest first.
TABLES = [
    ("dicionario", 144),
    ("permit_state_annual", 12_136),
    ("permit_state_monthly", 120_380),
    ("permit_msa_annual", 31_428),
    ("permit_msa_monthly", 266_304),
    ("permit_cbsa_annual", 38_720),
    ("permit_cbsa_monthly", 466_820),
    ("permit_county_annual", 435_772),
    ("permit_county_monthly", 1_510_676),
    ("permit_place_annual", 3_515_268),
    ("permit_place_monthly", 19_283_716),
]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("tables", nargs="*")
    args = parser.parse_args()
    gcs, bq = clients()
    bq.create_dataset(f"{PROJECT}.{DATASET_ID}_staging", exists_ok=True)
    todo = [t for t in TABLES if not args.tables or t[0] in args.tables]
    for table, expected in todo:
        print(f"=== {table} ===", flush=True)
        upload_table(gcs, bq, table, expected)
    print("ALL TABLES UPLOADED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
