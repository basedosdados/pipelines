"""Upload cleaned au_dcceew_greenhouse parquet tables to BigQuery.

Usage:
    uv run python models/au_dcceew_greenhouse/code/upload.py [--env dev|prod] [table_slug ...]

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
DATASET_ID = "au_dcceew_greenhouse"
OUTPUT_ROOT = (
    Path(
        os.environ.get(
            "AU_DCCEEW_GREENHOUSE_DATA",
            Path.home() / "Downloads" / "au_dcceew_greenhouse_data",
        )
    )
    / "output"
)

# Monkey-patch for requester-pays bucket
_orig_bucket = gcs.Client.bucket


def _patched_bucket(self, bucket_name, user_project=None):
    """Force the billing project: the staging bucket is requester-pays."""
    return _orig_bucket(self, bucket_name, user_project=BILLING_PROJECT)


gcs.Client.bucket = _patched_bucket

# Smallest first.
TABLES = ["inventory_scope2", "inventory_anzsic", "inventory_unfccc"]


def local_rows(path: Path) -> int:
    return sum(
        pq.read_metadata(f).num_rows
        for f in sorted(path.glob("year=*/data.parquet"))
    )


def upload_table(slug: str) -> int:
    path = OUTPUT_ROOT / slug
    if not path.exists():
        raise FileNotFoundError(f"Missing output path: {path}")

    tb = bd.Table(dataset_id=DATASET_ID, table_id=slug)

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
    expected = local_rows(path)
    status = "OK" if n == expected else "ROW MISMATCH"
    print(f"  {slug}: uploaded {n:,} rows (local {expected:,}) — {status}")
    if n != expected:
        raise ValueError(f"{slug}: row count {n:,} != local {expected:,}")
    return n


def main():
    only = set(_argv)
    unknown = sorted(only - set(TABLES))
    if unknown:
        raise SystemExit(
            f"unknown table: {', '.join(unknown)}\nvalid: {', '.join(TABLES)}"
        )
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
