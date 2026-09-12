"""Upload cleaned parquet tables of us_cfpb_hmda to BigQuery staging (basedosdados-dev).

  uv run python upload.py                 # all tables, enforcing full year coverage
  uv run python upload.py loan_application_register        # one table
  HMDA_ALLOW_PARTIAL=1 uv run python upload.py loan_application_register  # skip completeness gate

loan_application_register / _legacy are hive-partitioned by year (year=<YYYY>/data.parquet);
dicionario is a single file. Verifies each staging table's BigQuery row count against the
local parquet row count and stops at the first mismatch.

Requires GOOGLE_APPLICATION_CREDENTIALS pointing at a BD dev service-account key plus
~/.basedosdados/config.toml. The GCS bucket is requester-pays, so gcs.Client.bucket is
monkeypatched to pin user_project to the billing project.
"""

import glob
import os
import sys

import basedosdados as bd
import google.cloud.storage as gcs
import pyarrow.parquet as pq
from common import LEGACY, LEGACY_YEARS, MODERN, MODERN_YEARS, OUTPUT

BILLING_PROJECT = "basedosdados-dev"
DATASET_ID = "us_cfpb_hmda"
TABLES = [MODERN, LEGACY, "dicionario"]
EXPECTED_FILES = {
    MODERN: len(MODERN_YEARS),
    LEGACY: len(LEGACY_YEARS),
    "dicionario": 1,
}
ALLOW_PARTIAL = os.environ.get("HMDA_ALLOW_PARTIAL") == "1"

_orig_bucket = gcs.Client.bucket


def _patched_bucket(self, bucket_name, user_project=None):
    return _orig_bucket(self, bucket_name, user_project=BILLING_PROJECT)


gcs.Client.bucket = _patched_bucket


def local_rows(table_slug: str) -> tuple[int, int]:
    files = glob.glob(
        str(OUTPUT / table_slug / "**" / "*.parquet"), recursive=True
    )
    return sum(pq.ParquetFile(f).metadata.num_rows for f in files), len(files)


def check_complete(table_slug: str, nfiles: int) -> None:
    want = EXPECTED_FILES[table_slug]
    if nfiles != want and not ALLOW_PARTIAL:
        raise ValueError(
            f"Incomplete local export for {table_slug}: found {nfiles} parquet file(s), "
            f"expected {want}. Finish clean.py or set HMDA_ALLOW_PARTIAL=1."
        )


def upload_table(table_slug: str) -> int:
    path = OUTPUT / table_slug
    expected, nfiles = local_rows(table_slug)
    print(
        f"[{table_slug}] local: {expected:,} rows across {nfiles} parquet file(s)"
    )
    check_complete(table_slug, nfiles)

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
    ok = "MATCH" if n == expected else "MISMATCH"
    print(f"[{table_slug}] uploaded - bq={n:,} expected={expected:,} {ok}")
    if n != expected:
        raise ValueError(f"row count mismatch for {table_slug}")
    return n


if __name__ == "__main__":
    tables = sys.argv[1:] or TABLES
    for t in tables:
        if t not in EXPECTED_FILES:
            raise SystemExit(f"unknown table {t!r}")
        upload_table(t)
    print("done.")
