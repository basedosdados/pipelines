"""Upload the cleaned parquet to the ``basedosdados-dev`` staging dataset.

    uv run python models/fr_meteofrance/code/upload.py [table_slug ...]

``synop`` is hive-partitioned by ``ano``; the others are a single parquet file.
Every table is verified by row count after upload, and the run stops at the first
mismatch rather than continuing to the next table.
"""

import os
import sys
from pathlib import Path

import google.cloud.storage as gcs
from basedosdados import Storage, Table
from google.cloud import bigquery

BILLING_PROJECT = "basedosdados-dev"
DATASET_ID = "fr_meteofrance"
STAGING_DATASET = f"{DATASET_ID}_staging"
OUTPUT_ROOT = Path(
    os.path.expanduser(
        os.environ.get("MF_OUTPUT", "~/Downloads/fr_meteofrance_data/output")
    )
)

# The GCS bucket is requester-pays, so every bucket handle needs a billing project.
_orig_bucket = gcs.Client.bucket


def _patched_bucket(self, bucket_name, user_project=None):
    return _orig_bucket(self, bucket_name, user_project=BILLING_PROJECT)


gcs.Client.bucket = _patched_bucket

TABLES = [
    "dicionario",
    "station_synop",
    "station_climatologique",
    "normale_climatologique",
    "synop",
]


def expected_rows(slug):
    """Row count read straight from the parquet footers, so it is independent of BigQuery."""
    import pyarrow.parquet as pq

    root = OUTPUT_ROOT / slug
    files = sorted(root.rglob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"no parquet under {root}")
    return sum(pq.ParquetFile(f).metadata.num_rows for f in files), root


def count_rows(slug):
    client = bigquery.Client(project=BILLING_PROJECT)
    query = f"SELECT COUNT(*) AS n FROM `{BILLING_PROJECT}.{STAGING_DATASET}.{slug}`"
    return next(iter(client.query(query).result())).n


def upload_one(slug):
    expected, path = expected_rows(slug)
    print(
        f"\n=== {slug} ===\npath: {path}\nexpected rows: {expected:,}",
        flush=True,
    )

    Storage(dataset_id=DATASET_ID, table_id=slug).delete_table(
        mode="staging", not_found_ok=True
    )
    print("staging prefix cleared", flush=True)

    Table(dataset_id=DATASET_ID, table_id=slug).create(
        path=str(path),
        source_format="parquet",
        if_table_exists="replace",
        if_storage_data_exists="replace",
    )
    print("staging table created and parquet uploaded", flush=True)

    got = count_rows(slug)
    print(f"row count in BigQuery: {got:,}", flush=True)
    if got != expected:
        raise AssertionError(
            f"{slug}: BigQuery has {got:,} rows, parquet has {expected:,}"
        )
    print(f"OK {slug}", flush=True)
    return got


def main():
    wanted = sys.argv[1:] or TABLES
    results = {}
    for slug in wanted:
        results[slug] = upload_one(slug)
    print("\n=== DONE ===")
    for slug, n in results.items():
        print(f"  {slug}: {n:,} rows")


if __name__ == "__main__":
    main()
