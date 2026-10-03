"""Upload the cleaned us_bls_cex parquet tables to BigQuery dev staging.

Usage:
    uv run python -m models.us_bls_cex.code.upload [table_slug ...]

Writes to basedosdados-dev only: prod tables are materialised by the
table-approve action when the PR merges, never uploaded by hand. Tables go
smallest first, and the run stops on the first failure or row-count mismatch.
Expected row counts are read from the parquet metadata of the local output.
"""

import sys
import warnings
from pathlib import Path

warnings.filterwarnings("ignore")

import basedosdados as bd  # noqa: E402
import google.cloud.storage as gcs  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402
from google.cloud import bigquery  # noqa: E402

from pipelines.datasets.us_bls_cex.pumd_files import OUTPUT_DIR  # noqa: E402

BILLING_PROJECT = "basedosdados-dev"
# the dev service account; user ADC lacks bigquery.jobs.create on dev
CREDENTIALS = Path.home() / ".basedosdados" / "credentials" / "staging.json"
DATASET_ID = "us_bls_cex"

TABLES = [
    "dicionario",
    "series",
    "ucc",
    "annual",
    "diary_income",
    "diary_member",
    "diary_household",
    "interview_member",
    "interview_household",
    "diary_expenditure",
    "interview_income",
    "interview_expenditure",
]

# Monkey-patch for the requester-pays bucket
_orig_bucket = gcs.Client.bucket


def _patched_bucket(self, bucket_name, user_project=None):
    return _orig_bucket(self, bucket_name, user_project=BILLING_PROJECT)


gcs.Client.bucket = _patched_bucket


def local_rows(slug: str) -> int:
    return sum(
        pq.ParquetFile(p).metadata.num_rows
        for p in (OUTPUT_DIR / slug).rglob("*.parquet")
    )


def upload_table(slug: str) -> int:
    path = OUTPUT_DIR / slug
    if not path.exists():
        raise FileNotFoundError(f"Missing output path: {path}")
    expected = local_rows(slug)

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

    client = bigquery.Client.from_service_account_json(
        str(CREDENTIALS), project=BILLING_PROJECT
    )
    q = f"select count(*) n from `{BILLING_PROJECT}.{DATASET_ID}_staging.{slug}`"
    n = next(iter(client.query(q).result())).n
    print(
        f"  {slug}: {n:,} rows in staging (expected {expected:,})", flush=True
    )
    if n != expected:
        raise ValueError(f"{slug}: row count {n:,} != expected {expected:,}")
    return n


def main():
    only = set(sys.argv[1:])
    print(f"=== uploading to {BILLING_PROJECT} ===", flush=True)
    for slug in [t for t in TABLES if not only or t in only]:
        print(f"=== {slug} ===", flush=True)
        try:
            upload_table(slug)
        except Exception as e:
            print(f"  FAILED: {type(e).__name__}: {e}")
            sys.exit(1)
    print("ALL TABLES UPLOADED")


if __name__ == "__main__":
    main()
