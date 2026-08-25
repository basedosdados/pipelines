"""Upload the cleaned us_bls_oes parquet tables to BigQuery.

Usage:
    uv run python models/us_bls_oes/code/upload.py [--env dev|prod] [table_slug ...]

--env dev (default) -> basedosdados-dev; --env prod -> basedosdados. Point
GOOGLE_APPLICATION_CREDENTIALS at the matching service account. Uploads
sequentially (smallest first), verifies the staging row count against the local
parquet, and stops on the first failure.

Row counts are read from the local output rather than hardcoded, so a re-clean
cannot silently drift from what this script expects.
"""

import os
import sys
import warnings
from pathlib import Path

warnings.filterwarnings("ignore")

import basedosdados as bd  # noqa: E402
import google.cloud.storage as gcs  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402
from google.cloud import bigquery  # noqa: E402

_argv = sys.argv[1:]
if "--env" in _argv:
    _i = _argv.index("--env")
    ENV = _argv[_i + 1]
    _argv = _argv[:_i] + _argv[_i + 2 :]
else:
    ENV = "dev"

BILLING_PROJECT = "basedosdados" if ENV == "prod" else "basedosdados-dev"
DATASET_ID = "us_bls_oes"
ROOT = Path(
    os.environ.get("OES_DATA_DIR", Path.home() / "Downloads/us_bls_oes_data")
)
OUTPUT_ROOT = Path(os.environ.get("OES_OUTPUT_DIR", ROOT / "output"))

# The GCS bucket is requester-pays.
_orig_bucket = gcs.Client.bucket


def _patched_bucket(self, bucket_name, user_project=None):
    return _orig_bucket(self, bucket_name, user_project=BILLING_PROJECT)


gcs.Client.bucket = _patched_bucket

# Smallest first, so a credentials or schema problem surfaces cheaply.
TABLES = ["dicionario", "industry", "area"]


def local_rows(path: Path) -> int:
    """Count rows across a table's parquet files without loading them."""
    files = sorted(path.rglob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"no parquet under {path}")
    return sum(pq.ParquetFile(f).metadata.num_rows for f in files)


def upload_table(slug: str) -> int:
    path = OUTPUT_ROOT / slug
    expected = local_rows(path)

    # Clear the stale staging prefix first: leftover objects from an earlier
    # run would otherwise be read by the external table alongside the new ones.
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
    if n != expected:
        raise ValueError(
            f"{slug}: staging has {n:,} rows, local has {expected:,}"
        )
    print(f"  {slug}: {n:,} rows — OK")
    return n


def main():
    only = set(_argv)
    tables = [t for t in TABLES if not only or t in only]
    print(f"=== uploading to {BILLING_PROJECT} (env={ENV}) ===", flush=True)
    for slug in tables:
        print(f"=== {slug} ===", flush=True)
        try:
            upload_table(slug)
        except Exception as exc:
            print(f"  FAILED: {type(exc).__name__}: {exc}")
            sys.exit(1)
    print("ALL TABLES UPLOADED")


if __name__ == "__main__":
    main()
