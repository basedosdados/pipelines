"""Upload the cleaned br_mj_sisdepen parquet to BigQuery (dev project).

    python models/br_mj_sisdepen/code/upload.py [--tables t1,t2] [--dry-run]

Targets basedosdados-dev only. Production tables are materialised by the
table-approve GitHub action when the onboarding PR merges, never from here.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

# The SDK's own service-account key is the dev uploader credential; fall back to
# it when no Application Default Credentials are configured on the machine.
_SDK_KEY = Path.home() / ".basedosdados" / "credentials" / "staging.json"
if not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") and _SDK_KEY.exists():
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(_SDK_KEY)

# imported after the credential is set: the SDK resolves credentials at import
import basedosdados as bd  # noqa: E402
from google.cloud import storage  # noqa: E402

from models.br_mj_sisdepen.code.constants import (  # noqa: E402
    OUTPUT_DIR,
    TABLES,
)

GCP_DATASET_ID = "br_mj_sisdepen"
BILLING_PROJECT = "basedosdados-dev"
BUCKET = "basedosdados-dev"

# The GCS bucket is requester-pays, so every client must name a billing project.
_original_bucket = storage.Client.bucket


def _bucket_with_user_project(self, bucket_name, user_project=None, **kwargs):
    return _original_bucket(
        self, bucket_name, user_project=BILLING_PROJECT, **kwargs
    )


storage.Client.bucket = _bucket_with_user_project


def delete_staging_prefix(table_slug: str) -> int:
    """Remove a stale staging prefix before upload.

    Without this, a re-upload whose partitions differ from the previous run
    leaves orphan files behind and BigQuery raises a partition-key conflict.
    """
    client = storage.Client(project=BILLING_PROJECT)
    bucket = client.bucket(BUCKET)
    prefix = f"staging/{GCP_DATASET_ID}/{table_slug}/"
    blobs = list(client.list_blobs(bucket, prefix=prefix))
    for blob in blobs:
        blob.delete()
    return len(blobs)


def upload_table(table_slug: str, dry_run: bool = False) -> None:
    path = OUTPUT_DIR / table_slug
    if not path.exists():
        raise SystemExit(f"missing cleaned output for {table_slug}: {path}")
    files = sorted(path.rglob("*.parquet"))
    print(f"\n=== {table_slug}: {len(files)} parquet file(s) under {path}")
    if dry_run:
        return
    removed = delete_staging_prefix(table_slug)
    print(f"  cleared {removed} stale staging object(s)")
    table = bd.Table(table_id=table_slug, dataset_id=GCP_DATASET_ID)
    table.create(
        path=str(path),
        if_table_exists="replace",
        if_storage_data_exists="replace",
        source_format="parquet",
    )
    print(f"  created staging table {GCP_DATASET_ID}_staging.{table_slug}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--tables", default=",".join(TABLES))
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    for slug in [t.strip() for t in args.tables.split(",") if t.strip()]:
        upload_table(slug, args.dry_run)
    print("\ndone")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
