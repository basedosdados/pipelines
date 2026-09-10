"""Upload cleaned au_abs_prices_inflation parquet tables to BigQuery.

Usage:
    uv run python models/au_abs_prices_inflation/code/upload.py [--env dev|prod] [table_slug ...]

--env dev (default) -> basedosdados-dev; --env prod -> basedosdados. Point
GOOGLE_APPLICATION_CREDENTIALS at the matching service account. Uploads
sequentially (smallest first) and stops on first failure.
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
DATASET_ID = "au_abs_prices_inflation"
# Scratch data (raw downloads, cleaned parquet) never lives in the repo: the
# checkout sits inside Dropbox, so writing multi-GB output here would trigger a
# sync and risk committing data. Default to ~/Downloads and allow an override.
OUTPUT_ROOT = (
    Path(
        os.environ.get(
            "AU_ABS_PRICES_INFLATION_DATA",
            Path.home() / "Downloads" / "au_abs_prices_inflation_data" / "cpi",
        )
    )
    / "output"
)

# Monkey-patch for requester-pays bucket
_orig_bucket = gcs.Client.bucket


def _patched_bucket(self, bucket_name, user_project=None):
    return _orig_bucket(self, bucket_name, user_project=BILLING_PROJECT)


gcs.Client.bucket = _patched_bucket

# Smallest first.
TABLES = ["cpi_quarterly", "cpi_monthly"]


def local_rows(path: Path) -> int:
    """Row count of the cleaned parquet about to be uploaded."""
    return sum(
        pq.read_metadata(f).num_rows
        for f in sorted(path.glob("year=*/data.parquet"))
    )


def upload_table(slug: str) -> int:
    path = OUTPUT_ROOT / slug
    if not path.exists():
        raise FileNotFoundError(f"Missing output path: {path}")

    tb = bd.Table(dataset_id=DATASET_ID, table_id=slug)

    # Delete stale GCS staging prefix (avoids BQ partition key conflicts)
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

    # Compare against the parquet actually on disk, not a frozen literal: ABS
    # republishes the full history every release, so a hardcoded expectation
    # goes stale the moment a new period lands. What must hold is that the
    # upload lost nothing.
    expected_rows = local_rows(path)
    status = "OK" if n == expected_rows else "ROW MISMATCH"
    print(
        f"  {slug}: uploaded {n:,} rows (local {expected_rows:,}) — {status}"
    )
    if n != expected_rows:
        raise ValueError(f"{slug}: row count {n:,} != local {expected_rows:,}")
    return n


def main():
    only = set(_argv)
    tables = [s for s in TABLES if not only or s in only]
    print(f"=== uploading to {BILLING_PROJECT} (env={ENV}) ===", flush=True)
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
