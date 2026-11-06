"""Upload cleaned us_nyuad_cora parquet to the BigQuery dev staging dataset.

Usage:
    uv run --no-sync python models/us_nyuad_cora/code/upload.py [table_slug ...]

Targets ``basedosdados-dev`` only (prod tables are materialised by the merge).
Reads ``$US_NYUAD_CORA_DATA_DIR/output/<table>/`` and the row count recorded by
``clean_data.py`` in ``output/_manifest_<table>.json``; stops on the first
table whose staging row count does not match.
"""

import json
import os
import sys
import warnings
from pathlib import Path

warnings.filterwarnings("ignore")

import basedosdados as bd  # noqa: E402
import google.cloud.storage as gcs  # noqa: E402
from google.cloud import bigquery  # noqa: E402

BILLING_PROJECT = "basedosdados-dev"
DATASET_ID = "us_nyuad_cora"
OUTPUT_ROOT = (
    Path(
        os.environ.get(
            "US_NYUAD_CORA_DATA_DIR",
            Path.home() / "Library" / "Caches" / "us_nyuad_cora_data",
        )
    )
    / "output"
)

# Monkey-patch for the requester-pays bucket.
_orig_bucket = gcs.Client.bucket


def _patched_bucket(self, bucket_name, user_project=None):
    return _orig_bucket(self, bucket_name, user_project=BILLING_PROJECT)


gcs.Client.bucket = _patched_bucket

TABLES = ["speech"]


def upload_table(slug: str) -> int:
    path = OUTPUT_ROOT / slug
    manifest = OUTPUT_ROOT / f"_manifest_{slug}.json"
    if not path.exists() or not manifest.exists():
        raise FileNotFoundError(f"Missing output or manifest for {slug}")
    expected = json.loads(manifest.read_text())["rows"]

    # Delete stale GCS staging prefix (avoids leftovers from earlier runs).
    st = bd.Storage(dataset_id=DATASET_ID, table_id=slug)
    try:
        st.delete_table(mode="staging", not_found_ok=True)
    except Exception as e:
        print(f"  [warn] staging prefix cleanup: {e}")

    bd.Table(dataset_id=DATASET_ID, table_id=slug).create(
        path=str(path),
        source_format="parquet",
        if_table_exists="replace",
        if_storage_data_exists="replace",
        if_dataset_exists="pass",
    )

    client = bigquery.Client(project=BILLING_PROJECT)
    q = f"select count(*) as n from `{BILLING_PROJECT}.{DATASET_ID}_staging.{slug}`"
    n = next(iter(client.query(q).result())).n
    status = "OK" if n == expected else "ROW MISMATCH"
    print(
        f"  {slug}: staging {n:,} rows (expected {expected:,}) — {status}",
        flush=True,
    )
    if n != expected:
        raise ValueError(f"{slug}: row count {n:,} != expected {expected:,}")
    return n


def main():
    only = sys.argv[1:]
    tables = [t for t in TABLES if not only or t in only]
    print(f"=== uploading to {BILLING_PROJECT} ===", flush=True)
    for slug in tables:
        print(f"=== {slug} ===", flush=True)
        try:
            upload_table(slug)
        except Exception as e:
            print(f"  FAILED: {type(e).__name__}: {e}")
            sys.exit(1)
    print("ALL TABLES UPLOADED")


if __name__ == "__main__":
    main()
