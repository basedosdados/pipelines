"""Upload cleaned IPEDS parquet tables to BigQuery dev (basedosdados-dev).

Usage:
    uv run python models/us_ed_ipeds/code/upload.py [table_slug ...]

Uploads sequentially (smallest first). Stops on first failure.
"""

import sys
import warnings
from pathlib import Path

warnings.filterwarnings("ignore")

import basedosdados as bd  # noqa: E402
import google.cloud.storage as gcs  # noqa: E402
from google.cloud import bigquery  # noqa: E402

BILLING_PROJECT = "basedosdados-dev"
DATASET_ID = "us_ed_ipeds"
OUTPUT_ROOT = Path(__file__).resolve().parent.parent / "output"

# Monkey-patch for requester-pays bucket
_orig_bucket = gcs.Client.bucket


def _patched_bucket(self, bucket_name, user_project=None):
    return _orig_bucket(self, bucket_name, user_project=BILLING_PROJECT)


gcs.Client.bucket = _patched_bucket

# (table_slug, expected_rows) — smallest first
TABLES = [
    ("adm", 20_571),
    ("al", 39_423),
    ("f1a", 42_334),
    ("f2", 42_954),
    ("f3", 59_487),
    ("ic_py", 61_317),
    ("gr200", 88_248),
    ("ic_ay", 90_151),
    ("sfa", 132_928),
    ("ef_d", 145_782),
    ("flags", 147_769),
    ("efia", 154_783),
    ("hd", 162_442),
    ("ic", 174_441),
    ("sal_is", 194_576),
    ("om", 368_149),
    ("effy", 840_872),
    ("gr", 1_200_909),
    ("ef_c", 1_356_739),
    ("ef_a", 2_830_464),
    ("ef_b", 3_288_967),
    ("c_a", 6_581_665),
    ("eap", 8_714_580),
]


def upload_table(slug: str, expected_rows: int) -> int:
    path = OUTPUT_ROOT / slug
    if not path.exists():
        raise FileNotFoundError(f"Missing output path: {path}")

    tb = bd.Table(dataset_id=DATASET_ID, table_id=slug)

    # Delete stale GCS staging prefix
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

    status = "OK" if n == expected_rows else "ROW MISMATCH"
    print(
        f"  {slug}: uploaded {n:,} rows (expected {expected_rows:,}) — {status}"
    )
    if n != expected_rows:
        raise ValueError(
            f"{slug}: row count {n:,} != expected {expected_rows:,}"
        )
    return n


def main():
    only = set(sys.argv[1:])
    tables = [(s, r) for s, r in TABLES if not only or s in only]
    for slug, expected in tables:
        print(f"=== {slug} ===", flush=True)
        try:
            upload_table(slug, expected)
        except Exception as e:
            print(f"  FAILED: {type(e).__name__}: {e}")
            sys.exit(1)
    print("ALL TABLES UPLOADED")


if __name__ == "__main__":
    main()
