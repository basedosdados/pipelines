"""Upload the cleaned au_abs_population parquet tables to BigQuery.

Usage:
    uv run python models/au_abs_population/code/upload.py [table_slug ...]

Targets basedosdados-dev only. Production tables are materialised by the
table-approve GitHub action when the onboarding PR merges; they are never
uploaded from a laptop. Uploads smallest first and stops on the first failure.

Scratch data lives under ~/Downloads/au_abs_population_data (override with
AU_ABS_POPULATION_DATA), never inside the repo or Dropbox.
"""

import os
import sys
import warnings
from pathlib import Path

warnings.filterwarnings("ignore")

import basedosdados as bd  # noqa: E402
import google.cloud.storage as gcs  # noqa: E402
from google.cloud import bigquery  # noqa: E402

BILLING_PROJECT = "basedosdados-dev"
DATASET_ID = "au_abs_population"
OUTPUT_ROOT = (
    Path(
        os.environ.get(
            "AU_ABS_POPULATION_DATA",
            os.path.expanduser("~/Downloads/au_abs_population_data"),
        )
    )
    / "output"
)

# The staging bucket is requester-pays, so every client needs a billing project.
_orig_bucket = gcs.Client.bucket


def _patched_bucket(self, bucket_name, user_project=None):
    return _orig_bucket(self, bucket_name, user_project=BILLING_PROJECT)


gcs.Client.bucket = _patched_bucket

# (table_slug, expected_rows) — smallest first. The counts come from
# validate.py and are asserted after every upload.
TABLES = [
    ("series", 9_921),
    ("regional_lga", 13_700),
    ("national_state", 15_563),
    ("regional_sa2", 61_335),
    ("erp_age_sex", 149_985),
    ("projection", 355_050),
]


def upload_table(slug: str, expected_rows: int) -> int:
    path = OUTPUT_ROOT / slug
    if not path.exists():
        raise FileNotFoundError(f"Missing output path: {path}")

    tb = bd.Table(dataset_id=DATASET_ID, table_id=slug)

    # Clear the stale staging prefix first: the upload writes by object name, so
    # a partition left behind by an earlier run would survive and still be read
    # by the external table.
    st = bd.Storage(dataset_id=DATASET_ID, table_id=slug)
    try:
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

    status = "OK" if n == expected_rows else "ROW MISMATCH"
    print(
        f"  {slug}: uploaded {n:,} rows (expected {expected_rows:,}) - {status}"
    )
    if n != expected_rows:
        raise ValueError(
            f"{slug}: row count {n:,} != expected {expected_rows:,}"
        )
    return n


def main():
    only = set(sys.argv[1:])
    tables = [(s, r) for s, r in TABLES if not only or s in only]
    print(f"=== uploading to {BILLING_PROJECT} ===", flush=True)
    for slug, expected in tables:
        print(f"=== {slug} ===", flush=True)
        try:
            upload_table(slug, expected)
        except Exception as e:
            print(f"  FAILED: {type(e).__name__}: {e}")
            sys.exit(1)
    print("ALL TABLES UPLOADED")


if __name__ == "__main__":
    main()
