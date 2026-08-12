"""Upload cleaned world_cepii_baci / trade-directory parquet to BigQuery dev staging.

Usage:
    uv run python models/world_cepii_baci/code/upload.py [table_slug ...]

Point GOOGLE_APPLICATION_CREDENTIALS at the basedosdados-dev key (staging.json).
Uploads sequentially (smallest first); stops on first failure.
"""

import sys
import warnings
from pathlib import Path

warnings.filterwarnings("ignore")

import basedosdados as bd  # noqa: E402
import google.cloud.storage as gcs  # noqa: E402
from google.cloud import bigquery  # noqa: E402

BILLING_PROJECT = "basedosdados-dev"
OUTPUT_ROOT = Path.home() / "Downloads" / "world_cepii_baci_data" / "output"

# Monkey-patch for requester-pays bucket
_orig_bucket = gcs.Client.bucket


def _patched_bucket(self, bucket_name, user_project=None):
    return _orig_bucket(self, bucket_name, user_project=BILLING_PROJECT)


gcs.Client.bucket = _patched_bucket

# (dataset_id, table_slug, expected_rows) — smallest first
TABLES = [
    ("br_bd_diretorios_comercio_internacional", "hs1992", 5_022),
    ("br_bd_diretorios_comercio_internacional", "hs2017", 5_384),
    ("world_cepii_baci", "trade_hs17", 89_207_221),
    ("world_cepii_baci", "trade_hs92", 269_894_500),
]


def upload_table(dataset_id: str, slug: str, expected_rows: int) -> int:
    path = OUTPUT_ROOT / slug
    if not path.exists():
        raise FileNotFoundError(f"Missing output path: {path}")

    st = bd.Storage(dataset_id=dataset_id, table_id=slug)
    try:
        st.delete_table(mode="staging", not_found_ok=True)
    except Exception as e:
        print(f"  [warn] staging prefix cleanup: {e}")

    bd.Table(dataset_id=dataset_id, table_id=slug).create(
        path=str(path),
        source_format="parquet",
        if_table_exists="replace",
        if_storage_data_exists="replace",
        if_dataset_exists="pass",
    )

    client = bigquery.Client(project=BILLING_PROJECT)
    q = f"select count(*) as n from `{BILLING_PROJECT}.{dataset_id}_staging.{slug}`"
    n = next(iter(client.query(q).result())).n
    status = "OK" if n == expected_rows else "ROW MISMATCH"
    print(
        f"  {dataset_id}.{slug}: {n:,} rows (expected {expected_rows:,}) — {status}"
    )
    if n != expected_rows:
        raise ValueError(f"{slug}: {n:,} != expected {expected_rows:,}")
    return n


def main():
    only = set(sys.argv[1:])
    tables = [t for t in TABLES if not only or t[1] in only]
    print(f"=== uploading to {BILLING_PROJECT} ===", flush=True)
    for dataset_id, slug, expected in tables:
        print(f"=== {dataset_id}.{slug} ===", flush=True)
        try:
            upload_table(dataset_id, slug, expected)
        except Exception as e:
            print(f"  FAILED: {type(e).__name__}: {e}")
            sys.exit(1)
    print("DONE")


if __name__ == "__main__":
    main()
