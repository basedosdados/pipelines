"""Upload cleaned Parquet tables of au_alexander_politicians to BigQuery dev.

Uploads each table to gs://basedosdados-dev staging and creates
basedosdados-dev.au_alexander_politicians_staging.<table_slug>, then verifies
row counts. One-shot onboarding => typed Parquet. Stops at the first failure.

Run:  python upload.py            # all tables
      python upload.py politician  # a subset
"""

import sys
from pathlib import Path

import basedosdados as bd
import google.cloud.storage as gcs

BILLING_PROJECT = "basedosdados-dev"
DATASET_ID = "au_alexander_politicians"
ROOT = (
    Path(__file__).resolve().parent.parent
)  # models/au_alexander_politicians
OUTPUT_DIR = ROOT / "code" / "output"

# Monkey-patch for requester-pays bucket
_orig_bucket = gcs.Client.bucket


def _patched_bucket(self, bucket_name, user_project=None):
    return _orig_bucket(self, bucket_name, user_project=BILLING_PROJECT)


gcs.Client.bucket = _patched_bucket

# (table_slug, expected_row_count) — verified in clean.py against source CSVs
TABLES = [
    ("politician", 1783),
    ("party_affiliation", 2264),
    ("house_member", 1430),
    ("senator", 696),
    ("ministry", 2920),
]


def upload_table(table_slug: str, expected_rows: int) -> int:
    path = OUTPUT_DIR / f"{table_slug}.parquet"
    if not path.exists():
        raise FileNotFoundError(f"Missing parquet file: {path}")

    # Delete stale GCS staging prefix before upload
    st = bd.Storage(dataset_id=DATASET_ID, table_id=table_slug)
    st.delete_table(mode="staging", not_found_ok=True)

    tb = bd.Table(dataset_id=DATASET_ID, table_id=table_slug)
    tb.create(
        path=str(path),
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
