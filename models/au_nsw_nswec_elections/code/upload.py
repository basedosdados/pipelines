"""Upload the cleaned Parquet to the BigQuery dev staging dataset.

Targets ``basedosdados-dev`` only. Production table data is materialised by the
table-approve action when the onboarding PR merges, never uploaded from here.

Usage::

    PYTHONPATH=. python models/au_nsw_nswec_elections/code/upload.py [table ...]
"""

from __future__ import annotations

import os
import pathlib
import sys
import time

import basedosdados as bd
from google.cloud import storage

from pipelines.datasets.au_nsw_nswec_elections.schema import TABLES

DATASET_ID = "au_nsw_nswec_elections"
BILLING_PROJECT = "basedosdados-dev"
BUCKET = "basedosdados-dev"

DATA_DIR = pathlib.Path(
    os.environ.get(
        "NSWEC_DATA_DIR",
        str(pathlib.Path.home() / "Downloads" / "au_nsw_nswec_elections_data"),
    )
)
OUTPUT = DATA_DIR / "output"

# The bucket is requester-pays, so every client must name a billing project.
_original_bucket = storage.Client.bucket


def _billed_bucket(self, bucket_name, user_project=None):
    return _original_bucket(self, bucket_name, user_project=BILLING_PROJECT)


storage.Client.bucket = _billed_bucket


def log(message: str) -> None:
    print(message, flush=True)


def clear_staging(table: str) -> None:
    """Drop the stale GCS prefix before uploading.

    Leftover objects from an earlier run collide with the new partition keys and
    BigQuery reports a partition conflict rather than an obviously stale file.
    """
    client = storage.Client(project=BILLING_PROJECT)
    bucket = client.bucket(BUCKET)
    prefix = f"staging/{DATASET_ID}/{table}/"
    blobs = list(client.list_blobs(bucket, prefix=prefix))
    for blob in blobs:
        blob.delete()
    if blobs:
        log(f"    cleared {len(blobs)} stale objects under {prefix}")


def main(argv: list[str]) -> int:
    wanted = [t for t in TABLES if t in set(argv[1:])] or list(TABLES)
    for table in wanted:
        path = OUTPUT / table
        if not path.exists():
            log(f"  {table}: no output, skipped")
            continue
        start = time.time()
        log(
            f"  {table}: uploading {sum(1 for _ in path.rglob('*.parquet'))} files"
        )
        clear_staging(table)
        handle = bd.Table(table_id=table, dataset_id=DATASET_ID)
        handle.create(
            path=str(path),
            if_table_exists="replace",
            if_storage_data_exists="replace",
            source_format="parquet",
        )
        log(f"  {table}: done in {time.time() - start:.1f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
