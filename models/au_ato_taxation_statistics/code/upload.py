"""Upload cleaned au_ato_taxation_statistics parquet to BigQuery staging.

Usage::

    GOOGLE_APPLICATION_CREDENTIALS=~/.basedosdados/credentials/staging.json \
      uv run python models/au_ato_taxation_statistics/code/upload.py [table ...]

Writes to ``basedosdados-dev`` only. Production table data is materialised
by the table-approve action when the onboarding PR merges, never from here.
"""

from __future__ import annotations

import os
import sys
import warnings
from pathlib import Path

warnings.filterwarnings("ignore")

import basedosdados as bd  # noqa: E402
import google.cloud.storage as gcs  # noqa: E402
from google.cloud import bigquery  # noqa: E402

BILLING_PROJECT = "basedosdados-dev"
DATASET_ID = "au_ato_taxation_statistics"
OUTPUT_ROOT = (
    Path(
        os.environ.get(
            "ATO_TAXSTATS_DATA",
            Path.home() / "Downloads" / "au_ato_taxation_statistics_data",
        )
    )
    / "output"
)

_orig_bucket = gcs.Client.bucket


def _patched_bucket(self, bucket_name, user_project=None):
    """The staging bucket is requester-pays, so pin the billing project."""
    return _orig_bucket(self, bucket_name, user_project=BILLING_PROJECT)


gcs.Client.bucket = _patched_bucket

# Smallest first so a credentials or schema problem surfaces cheaply.
TABLES = [
    "dicionario",
    "gst_industry",
    "company_industry",
    "individuals_industry",
    "individuals_income_state",
    "individuals_postcode",
]


def upload_table(slug: str) -> int:
    """Upload one table's parquet tree and return its staging row count."""
    path = OUTPUT_ROOT / slug
    if not path.exists():
        raise FileNotFoundError(f"missing output path: {path}")

    table = bd.Table(dataset_id=DATASET_ID, table_id=slug)
    storage = bd.Storage(dataset_id=DATASET_ID, table_id=slug)
    try:
        storage.delete_table(mode="staging", not_found_ok=True)
    except Exception as error:
        print(f"  [warn] staging prefix cleanup: {error}")

    table.create(
        path=str(path),
        source_format="parquet",
        if_table_exists="replace",
        if_storage_data_exists="replace",
        if_dataset_exists="pass",
    )

    client = bigquery.Client(project=BILLING_PROJECT)
    query = f"select count(*) as n from `{BILLING_PROJECT}.{DATASET_ID}_staging.{slug}`"
    rows = next(iter(client.query(query).result())).n
    print(f"  {slug}: {rows:,} rows in staging", flush=True)
    return rows


def main() -> None:
    """Upload every table, stopping at the first failure."""
    only = set(sys.argv[1:])
    tables = [t for t in TABLES if not only or t in only]
    print(f"=== uploading {tables} to {BILLING_PROJECT} ===", flush=True)
    for slug in tables:
        print(f"=== {slug} ===", flush=True)
        try:
            upload_table(slug)
        except Exception as error:
            print(f"  FAILED: {type(error).__name__}: {error}")
            sys.exit(1)
    print("ALL TABLES UPLOADED")


if __name__ == "__main__":
    main()
