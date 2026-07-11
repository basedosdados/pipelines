"""Upload cleaned ESS parquet tables to BigQuery dev (basedosdados-dev).

Usage:
    uv run python models/eu_ess/code/upload.py [round_NN ...]

Uploads each round table sequentially (smallest first), stops on first failure.
Expected row counts are read from the local parquet so this also handles rounds
added later (8-11). Requires GOOGLE_APPLICATION_CREDENTIALS to point at a valid
service-account key for the row-count verification query.
"""

import sys
import warnings
from pathlib import Path

warnings.filterwarnings("ignore")

import basedosdados as bd  # noqa: E402
import google.cloud.storage as gcs  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402
from google.cloud import bigquery  # noqa: E402

BILLING_PROJECT = "basedosdados-dev"
DATASET_ID = "eu_ess"
OUTPUT_ROOT = Path(__file__).resolve().parent.parent / "output"

# Requester-pays bucket: force user_project on every bucket() call.
_orig_bucket = gcs.Client.bucket


def _patched_bucket(self, bucket_name, user_project=None):
    return _orig_bucket(self, bucket_name, user_project=BILLING_PROJECT)


gcs.Client.bucket = _patched_bucket


def expected_rows(slug: str) -> int:
    # rglob catches both hive-partitioned (year=YYYY/data.parquet) round tables
    # and the flat, non-partitioned dicionario (data.parquet).
    total = 0
    for f in (OUTPUT_ROOT / slug).rglob("data.parquet"):
        total += pq.ParquetFile(f).metadata.num_rows
    return total


def upload_table(slug: str) -> int:
    path = OUTPUT_ROOT / slug
    if not path.exists():
        raise FileNotFoundError(f"Missing output path: {path}")
    expected = expected_rows(slug)

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
    print(f"  {slug}: uploaded {n:,} rows (expected {expected:,}) — {status}")
    if n != expected:
        raise ValueError(f"{slug}: row count {n:,} != expected {expected:,}")
    return n


def main():
    only = set(sys.argv[1:])
    slugs = sorted(
        (
            p.name
            for p in OUTPUT_ROOT.iterdir()
            if p.is_dir()
            and (p.name.startswith("round_") or p.name == "dicionario")
        ),
        key=lambda s: expected_rows(s),
    )
    slugs = [s for s in slugs if not only or s in only]
    for slug in slugs:
        print(f"=== {slug} ===", flush=True)
        try:
            upload_table(slug)
        except Exception as e:
            print(f"  FAILED: {type(e).__name__}: {e}")
            sys.exit(1)
    print("ALL TABLES UPLOADED")


if __name__ == "__main__":
    main()
