"""Upload the cleaned us_bls_employment parquet tables to BigQuery.

Usage:
    uv run python models/us_bls_employment/code/upload.py [--env dev|prod] [table ...]

``--env dev`` (default) targets ``basedosdados-dev``. Prod table data is
materialised by the table-approve action when the onboarding PR merges, not by
this script; ``--env prod`` exists only for the rare manual case.

Parquet is read from ``~/Downloads/us_bls_employment_data/output`` by default
(override with ``US_BLS_EMPLOYMENT_DATA``). Tables upload smallest first and the
run stops on the first failure, with the row count verified against the value
the cleaning step reported.
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
BILLING_PROJECT = "basedosdados" if ENV == "prod" else "basedosdados-dev"
DATASET_ID = "us_bls_employment"
OUTPUT_ROOT = (
    Path(
        os.environ.get(
            "US_BLS_EMPLOYMENT_DATA",
            os.path.expanduser("~/Downloads/us_bls_employment_data"),
        )
    )
    / "output"
)

# The GCS bucket is requester-pays, so every bucket handle needs a user project.
_orig_bucket = gcs.Client.bucket


def _patched_bucket(self, bucket_name, user_project=None):
    return _orig_bucket(self, bucket_name, user_project=BILLING_PROJECT)


gcs.Client.bucket = _patched_bucket

# (table slug, expected rows) — smallest first, from the cleaning run.
TABLES = [
    ("dicionario", 10_231),
    ("jolts", 626_683),
    ("ces_national", 8_359_938),
    ("ces_state_metro", 9_894_476),
    ("laus", 15_601_885),
]


def upload_table(slug: str, expected_rows: int) -> int:
    """Upload one table's partitioned parquet and verify its row count.

    Args:
        slug: Table slug, matching the output directory name.
        expected_rows: Row count the cleaning step reported.

    Returns:
        Rows present in the staging table after the upload.

    Raises:
        FileNotFoundError: If the table has not been cleaned.
        ValueError: If the staging row count does not match.
    """
    path = OUTPUT_ROOT / slug
    if not path.exists():
        raise FileNotFoundError(f"Missing output path: {path}")

    # A stale staging prefix makes BigQuery reject the new files on a partition
    # key conflict, so it is cleared before the upload rather than merged into.
    storage = bd.Storage(dataset_id=DATASET_ID, table_id=slug)
    try:
        storage.delete_table(mode="staging", not_found_ok=True)
    except Exception as exc:
        print(f"  [warn] staging prefix cleanup: {exc}")

    bd.Table(dataset_id=DATASET_ID, table_id=slug).create(
        path=str(path),
        source_format="parquet",
        if_table_exists="replace",
        if_storage_data_exists="replace",
        if_dataset_exists="pass",
    )

    client = bigquery.Client(project=BILLING_PROJECT)
    query = (
        f"select count(*) as n "
        f"from `{BILLING_PROJECT}.{DATASET_ID}_staging.{slug}`"
    )
    n = next(iter(client.query(query).result())).n
    ok = n == expected_rows
    print(
        f"  {slug}: uploaded {n:,} rows (expected {expected_rows:,}) — "
        f"{'OK' if ok else 'ROW MISMATCH'}",
        flush=True,
    )
    if not ok:
        raise ValueError(
            f"{slug}: row count {n:,} != expected {expected_rows:,}"
        )
    return n


def main() -> None:
    """Upload every requested table, stopping on the first failure."""
    only = set(_argv)
    tables = [(s, r) for s, r in TABLES if not only or s in only]
    print(f"=== uploading to {BILLING_PROJECT} (env={ENV}) ===", flush=True)
    for slug, expected in tables:
        print(f"=== {slug} ===", flush=True)
        try:
            upload_table(slug, expected)
        except Exception as exc:
            print(f"  FAILED: {type(exc).__name__}: {exc}")
            sys.exit(1)
    print("ALL TABLES UPLOADED")


if __name__ == "__main__":
    main()
