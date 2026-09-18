"""Upload cleaned us_epa_tri parquet tables to BigQuery staging (dev).

Usage:
    uv run python models/us_epa_tri/code/upload.py [--env dev] [table ...]

Dev (default) -> basedosdados-dev. Uploads smallest first, stops on the first
failure, and prints each table's staging row count so it can be checked against
the cleaning-step totals.

Prod data is never uploaded from here: the prod tables are materialised by the
table-approve action when the onboarding PR merges.
"""

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
    _i = _argv.index("--env")
    ENV = _argv[_i + 1]
    _argv = _argv[:_i] + _argv[_i + 2 :]
else:
    ENV = "dev"
if ENV != "dev":
    sys.exit(
        "only --env dev is supported: prod tables are built by table-approve"
    )
BILLING_PROJECT = "basedosdados-dev"
DATASET_ID = "us_epa_tri"
# Scratch data lives outside the repo (never under Dropbox); overridable via env.
OUTPUT_ROOT = (
    Path(
        os.environ.get(
            "TRI_DATA_DIR", Path.home() / "Downloads/us_epa_tri_data"
        )
    )
    / "output"
)

_orig_bucket = gcs.Client.bucket


def _patched_bucket(self, bucket_name, user_project=None):
    # The staging bucket is requester-pays.
    return _orig_bucket(self, bucket_name, user_project=BILLING_PROJECT)


gcs.Client.bucket = _patched_bucket

# smallest first, so a credential or schema problem surfaces cheaply
TABLES = ["dicionario", "chemical", "facility", "form", "release"]


def upload_table(slug: str) -> int:
    path = OUTPUT_ROOT / slug
    if not path.exists():
        raise FileNotFoundError(f"Missing output path: {path}")

    tb = bd.Table(dataset_id=DATASET_ID, table_id=slug)
    st = bd.Storage(dataset_id=DATASET_ID, table_id=slug)
    try:
        # Clear the staging prefix first, or stale blobs collide with the new
        # partition keys.
        st.delete_table(mode="staging", not_found_ok=True)
    except Exception as e:
        print(f"  [warn] staging prefix cleanup: {e}")

    tb.create(
        path=str(path),
        source_format="parquet",
        if_table_exists="replace",
        if_storage_data_exists="replace",
        if_dataset_exists="pass",
    )

    client = bigquery.Client(project=BILLING_PROJECT)
    q = f"select count(*) as n from `{BILLING_PROJECT}.{DATASET_ID}_staging.{slug}`"
    n = next(iter(client.query(q).result())).n
    print(f"  {slug}: {n:,} rows in staging", flush=True)
    return n


def main():
    only = set(_argv)
    tables = [t for t in TABLES if not only or t in only]
    print(
        f"=== uploading {tables} to {BILLING_PROJECT} (env={ENV}) ===",
        flush=True,
    )
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
