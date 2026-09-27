"""Upload cleaned world_oecd_revenue_statistics parquet tables to BigQuery.

Usage:
    uv run python models/world_oecd_revenue_statistics/code/upload.py [--env dev|prod] [table ...]

--env dev (default) -> basedosdados-dev; --env prod -> basedosdados. Uploads
smallest first, verifies the staging row count against the parquet, stops on first
failure. Never uploads prod data locally (prod tables are materialised by the
table-approve action on merge).
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
DATASET_ID = "world_oecd_revenue_statistics"
# Cleaned parquet lives under the scratch data dir (never in the repo/Dropbox).
DATA_DIR = Path(
    os.environ.get(
        "OECD_REV_DATA_DIR",
        Path.home() / "Downloads" / "world_oecd_revenue_statistics_data",
    )
)
OUTPUT_ROOT = DATA_DIR / "output"
TABLES = ["dicionario", "revenue"]  # smallest first

_orig_bucket = gcs.Client.bucket


def _patched_bucket(self, bucket_name, user_project=None):
    return _orig_bucket(self, bucket_name, user_project=BILLING_PROJECT)


gcs.Client.bucket = _patched_bucket


def parquet_rows(path: Path) -> int:
    return sum(
        pq.ParquetFile(p).metadata.num_rows for p in path.rglob("*.parquet")
    )


def upload_table(slug: str) -> int:
    path = OUTPUT_ROOT / slug
    if not path.exists():
        raise FileNotFoundError(f"missing output path: {path}")
    expected = parquet_rows(path)

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
    status = "OK" if n == expected else "ROW MISMATCH"
    print(f"  {slug}: uploaded {n:,} rows (parquet {expected:,}) — {status}")
    if n != expected:
        raise ValueError(f"{slug}: staging {n:,} != parquet {expected:,}")
    return n


def main():
    only = set(_argv)
    tables = [t for t in TABLES if not only or t in only]
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
