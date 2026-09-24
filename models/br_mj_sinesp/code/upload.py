"""Upload the cleaned parquet to BigQuery dev (basedosdados-dev).

Local credentials are dev-only. Production table data is materialised by the
table-approve action when the onboarding PR merges, never uploaded from here.

Run with the Data Basis service account, not personal ADC::

    GOOGLE_APPLICATION_CREDENTIALS=~/.basedosdados/credentials.json \
        python upload.py

Without it the storage client falls back to application-default credentials and
the staging listing fails with "does not have serviceusage.services.use access".
"""

from __future__ import annotations

import os
import sys

import basedosdados as bd
from constants import DATASET_ID, OUTPUT_DIR, TABLES
from google.cloud import storage

BILLING_PROJECT = "basedosdados-dev"
BUCKET = "basedosdados-dev"

os.environ.setdefault(
    "GOOGLE_APPLICATION_CREDENTIALS",
    os.path.expanduser("~/.basedosdados/credentials/staging.json"),
)

# The bucket is requester-pays; without a user_project every call 400s.
_orig_bucket = storage.Client.bucket


def _bucket(self, bucket_name, user_project=None):
    return _orig_bucket(
        self, bucket_name, user_project=user_project or BILLING_PROJECT
    )


storage.Client.bucket = _bucket


def clear_staging_prefix(table_slug: str) -> int:
    """Delete the stale staging prefix so partition keys cannot collide."""
    client = storage.Client(project=BILLING_PROJECT)
    bucket = client.bucket(BUCKET)
    prefix = f"staging/{DATASET_ID}/{table_slug}/"
    blobs = list(client.list_blobs(bucket, prefix=prefix))
    for b in blobs:
        b.delete()
    return len(blobs)


def upload_table(table_slug: str) -> None:
    path = os.path.join(OUTPUT_DIR, table_slug)
    if not os.path.isdir(path):
        path = os.path.join(OUTPUT_DIR, table_slug, "data.parquet")
    if not os.path.exists(path):
        raise FileNotFoundError(path)

    n = clear_staging_prefix(table_slug)
    print(f"[{table_slug}] cleared {n} stale staging blobs")

    tb = bd.Table(table_id=table_slug, dataset_id=DATASET_ID)
    tb.create(
        path=path,
        if_table_exists="replace",
        if_storage_data_exists="replace",
        source_format="parquet",
    )
    print(f"[{table_slug}] uploaded from {path}")


if __name__ == "__main__":
    targets = sys.argv[1:] or TABLES
    for t in targets:
        upload_table(t)
        print(f"[{t}] OK\n")
