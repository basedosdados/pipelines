"""Upload the cleaned us_fbi_cde parquet tables to BigQuery staging (dev).

Usage:
    uv run python models/us_fbi_cde/code/upload.py [--env dev] [table ...]

Uploads smallest first so a credential or schema problem surfaces on the
1,000-row dictionary rather than after the 200-million-row victim-offense
table, and stops on the first failure.

Prod data is never uploaded from here: the prod tables are materialised by the
table-approve action when the onboarding PR merges.
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

_argv = sys.argv[1:]
if "--env" in _argv:
    _index = _argv.index("--env")
    ENV = _argv[_index + 1]
    _argv = _argv[:_index] + _argv[_index + 2 :]
else:
    ENV = "dev"
if ENV != "dev":
    sys.exit(
        "only --env dev is supported: prod tables are built by table-approve"
    )

BILLING_PROJECT = "basedosdados-dev"
DATASET_ID = "us_fbi_cde"
OUTPUT_ROOT = (
    Path(
        os.environ.get(
            "US_FBI_CDE_DATA", Path.home() / "Downloads/us_fbi_cde_data"
        )
    )
    / "output"
)

_original_bucket = gcs.Client.bucket


def _patched_bucket(self, bucket_name, user_project=None):
    # The staging bucket is requester-pays.
    return _original_bucket(self, bucket_name, user_project=BILLING_PROJECT)


gcs.Client.bucket = _patched_bucket

TABLES = [
    "dicionario",
    "hate_crime",
    "agency",
    "victim_offender_relationship",
    "arrestee",
    "ucr_summary",
    "incident",
    "victim",
    "offender",
    "offense",
    "property",
    "victim_offense",
]


def upload_table(slug):
    path = OUTPUT_ROOT / slug
    if not path.exists():
        raise FileNotFoundError(f"missing output path: {path}")

    table = bd.Table(dataset_id=DATASET_ID, table_id=slug)
    storage = bd.Storage(dataset_id=DATASET_ID, table_id=slug)
    try:
        # Clear the staging prefix first, or stale blobs collide with the new
        # partition keys.
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


def main():
    wanted = _argv or TABLES
    unknown = [t for t in wanted if t not in TABLES]
    if unknown:
        sys.exit(f"unknown table(s): {unknown}")
    for slug in [t for t in TABLES if t in wanted]:
        print(f"uploading {slug} ...", flush=True)
        upload_table(slug)
    print("done")


if __name__ == "__main__":
    main()
