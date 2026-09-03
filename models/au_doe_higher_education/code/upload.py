"""Upload cleaned au_doe_higher_education parquet tables to BigQuery.

Usage:
    uv run python models/au_doe_higher_education/code/upload.py [table_slug ...]

Targets basedosdados-dev only: prod table data is materialised by the
table-approve action when the onboarding PR merges, never uploaded by hand.
The institution directory goes to br_bd_diretorios_au_staging.
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
DATASET_ID = "au_doe_higher_education"
DIRECTORY_DATASET_ID = "br_bd_diretorios_au"
OUTPUT_ROOT = (
    Path(
        os.environ.get(
            "AU_DOE_DATA",
            Path.home() / "Downloads" / "au_dese_higher_education_data",
        )
    )
    / "output"
)

_orig_bucket = gcs.Client.bucket


def _patched_bucket(self, bucket_name, user_project=None):
    """The staging bucket is requester-pays, so a user project is required."""
    return _orig_bucket(self, bucket_name, user_project=BILLING_PROJECT)


gcs.Client.bucket = _patched_bucket

# (dataset_id, table_slug, expected_rows) — smallest first, so a credential or
# permission problem surfaces on a cheap table.
TABLES = [
    (DIRECTORY_DATASET_ID, "higher_education_institution", 151),
    (DATASET_ID, "application_offer", 171),
    (DATASET_ID, "student_attrition_retention_success", 1_782),
    (DATASET_ID, "student_equity_group", 2_743),
    (DATASET_ID, "student_completion_rate", 4_383),
    (DATASET_ID, "student_equity_performance", 9_947),
    (DATASET_ID, "staff", 21_519),
    (DATASET_ID, "student_load", 156_308),
    (DATASET_ID, "award_course_completion", 157_709),
    (DATASET_ID, "student_enrolment", 391_446),
]


def upload_table(dataset_id: str, slug: str, expected_rows: int) -> int:
    path = OUTPUT_ROOT / slug
    if not path.exists():
        path = OUTPUT_ROOT / f"{slug}.parquet"
    if not path.exists():
        raise FileNotFoundError(f"missing output path for {slug}")

    storage = bd.Storage(dataset_id=dataset_id, table_id=slug)
    try:
        # A stale prefix makes BigQuery reject the new partition keys.
        storage.delete_table(mode="staging", not_found_ok=True)
    except Exception as error:
        print(f"  [warn] staging prefix cleanup: {error}")

    table = bd.Table(dataset_id=dataset_id, table_id=slug)
    table.create(
        path=str(path),
        source_format="parquet",
        if_table_exists="replace",
        if_storage_data_exists="replace",
        if_dataset_exists="pass",
    )

    client = bigquery.Client(project=BILLING_PROJECT)
    query = f"select count(*) as n from `{BILLING_PROJECT}.{dataset_id}_staging.{slug}`"
    rows = next(iter(client.query(query).result())).n
    status = "OK" if rows == expected_rows else "ROW MISMATCH"
    print(f"  {slug}: {rows:,} rows (expected {expected_rows:,}) — {status}")
    if rows != expected_rows:
        raise ValueError(
            f"{slug}: {rows:,} rows != expected {expected_rows:,}"
        )
    return rows


def main() -> None:
    only = set(sys.argv[1:])
    selected = [t for t in TABLES if not only or t[1] in only]
    print(f"=== uploading to {BILLING_PROJECT} ===", flush=True)
    for dataset_id, slug, expected in selected:
        print(f"=== {dataset_id}.{slug} ===", flush=True)
        try:
            upload_table(dataset_id, slug, expected)
        except Exception as error:
            print(f"  FAILED: {type(error).__name__}: {error}")
            sys.exit(1)
    print("ALL TABLES UPLOADED")


if __name__ == "__main__":
    main()
