"""Upload the cleaned world_aiddata_gcdf Parquet table to BigQuery (dev).

Reads the hive-partitioned Parquet under the scratch dir (default
~/Downloads/world_aiddata_gcdf_data/output, override with
WORLD_AIDDATA_GCDF_DATA) and uploads to basedosdados-dev staging.
"""

import argparse
import os
import sys
from pathlib import Path

import basedosdados as bd
import google.cloud.storage as gcs
from google.cloud import bigquery

BILLING_PROJECT = "basedosdados-dev"
DATASET_ID = "world_aiddata_gcdf"
DATA_DIR = Path(
    os.environ.get(
        "WORLD_AIDDATA_GCDF_DATA",
        str(Path.home() / "Downloads" / "world_aiddata_gcdf_data"),
    )
)
OUTPUT_DIR = DATA_DIR / "output"

EXPECTED_ROWS = {"projects": 20_985}

# Monkey-patch for requester-pays bucket
_orig_bucket = gcs.Client.bucket


def _patched_bucket(self, bucket_name, user_project=None):
    return _orig_bucket(self, bucket_name, user_project=BILLING_PROJECT)


gcs.Client.bucket = _patched_bucket


def delete_staging_prefix(table_id: str) -> int:
    client = gcs.Client(project=BILLING_PROJECT)
    bucket = client.bucket("basedosdados-dev")
    n = 0
    for prefix in (f"staging/{DATASET_ID}/{table_id}/",):
        for blob in list(client.list_blobs(bucket, prefix=prefix)):
            blob.delete()
            n += 1
    return n


def upload_table(table_id: str) -> None:
    path = OUTPUT_DIR / table_id
    if not path.is_dir():
        raise FileNotFoundError(f"Output path missing: {path}")

    print(f"--- {table_id} ---", flush=True)
    tb = bd.Table(table_id=table_id, dataset_id=DATASET_ID)

    deleted = delete_staging_prefix(table_id)
    print(f"Deleted {deleted} stale staging objects", flush=True)

    tb.create(
        path=str(path),
        source_format="parquet",
        if_table_exists="replace",
        if_storage_data_exists="replace",
    )
    print("create+upload: OK", flush=True)

    bq = bigquery.Client(project=BILLING_PROJECT)
    query = (
        f"select count(*) as n from "
        f"`{BILLING_PROJECT}.{DATASET_ID}_staging.{table_id}`"
    )
    n = next(iter(bq.query(query).result()))["n"]
    expected = EXPECTED_ROWS[table_id]
    status = "OK" if n == expected else "MISMATCH"
    print(f"rows: {n:,} (expected {expected:,}) — {status}", flush=True)
    if n != expected:
        raise RuntimeError(
            f"{table_id}: row count mismatch ({n} vs {expected})"
        )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", default=None, help="Upload only this table")
    args = parser.parse_args()

    tables = [args.table] if args.table else list(EXPECTED_ROWS)
    for table_id in tables:
        try:
            upload_table(table_id)
        except Exception as e:
            print(f"FAILED: {table_id}: {e}", flush=True)
            sys.exit(1)
    print("ALL DONE", flush=True)


if __name__ == "__main__":
    main()
