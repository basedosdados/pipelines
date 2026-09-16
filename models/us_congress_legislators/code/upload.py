"""Upload cleaned parquet tables of us_congress_legislators to BigQuery.

Uploads each table to the staging bucket and creates
<project>_staging.us_congress_legislators_staging.<table_slug> (via bd.Table),
then verifies row counts. Stops at the first failure.

Default billing/target project is basedosdados-dev (staging metadata path).
For the prod materialization path, run with env BILLING_PROJECT=basedosdados-staging
and GOOGLE_APPLICATION_CREDENTIALS pointing at the matching SA.
"""

import os
import sys
from pathlib import Path

import basedosdados as bd
import google.cloud.storage as gcs

BILLING_PROJECT = os.environ.get("BILLING_PROJECT", "basedosdados-dev")
DATASET_ID = "us_congress_legislators"
ROOT = Path(__file__).resolve().parent.parent
OUTPUT_DIR = ROOT / "output"

# Monkey-patch for requester-pays bucket
_orig_bucket = gcs.Client.bucket


def _patched_bucket(self, bucket_name, user_project=None):
    return _orig_bucket(self, bucket_name, user_project=BILLING_PROJECT)


gcs.Client.bucket = _patched_bucket

# (table_slug, expected_row_count) — upload in this order
TABLES = [
    ("legislator", 12767),
    ("term", 45532),
    ("social_media", 519),
    ("leadership_role", 156),
    ("committee", 559),
    ("committee_membership", 3879),
    ("district_office", 1312),
    ("executive_term", 131),
]


def upload_table(table_slug: str, expected_rows: int) -> int:
    path = OUTPUT_DIR / table_slug / "data.parquet"
    if not path.exists():
        raise FileNotFoundError(f"Missing parquet file: {path}")

    st = bd.Storage(dataset_id=DATASET_ID, table_id=table_slug)
    st.delete_table(mode="staging", not_found_ok=True)

    tb = bd.Table(dataset_id=DATASET_ID, table_id=table_slug)
    tb.create(
        path=path,
        source_format="parquet",
        if_table_exists="replace",
        if_storage_data_exists="replace",
        if_dataset_exists="pass",
    )

    query = (
        f"select count(*) as n from "
        f"`{BILLING_PROJECT}.{DATASET_ID}_staging.{table_slug}`"
    )
    df = bd.read_sql(query, billing_project_id=BILLING_PROJECT, from_file=True)
    n = int(df["n"].iloc[0])
    match = "MATCH" if n == expected_rows else "MISMATCH"
    print(
        f"[{table_slug}] uploaded — rows={n} expected={expected_rows} {match}"
    )
    if n != expected_rows:
        raise ValueError(
            f"Row count mismatch for {table_slug}: got {n}, expected {expected_rows}"
        )
    return n


def main() -> None:
    only = sys.argv[1:] or None
    for table_slug, expected in TABLES:
        if only and table_slug not in only:
            continue
        try:
            upload_table(table_slug, expected)
        except Exception as e:
            print(f"[{table_slug}] FAILED — {e}")
            sys.exit(1)
    print("All uploads complete.")


if __name__ == "__main__":
    main()
