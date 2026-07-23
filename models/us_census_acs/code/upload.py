"""Upload cleaned us_census_acs parquet tables to BigQuery dev (basedosdados-dev).

Usage:
    GOOGLE_APPLICATION_CREDENTIALS=~/.basedosdados/credentials/<cred>.json \
      uv run python models/us_census_acs/code/upload.py [table_slug ...]

Data lives OUTSIDE the repo (Dropbox) at /Users/rdahis/acs_data/output.
Uploads sequentially (smallest first). Stops on first failure. Row counts are
reported (and cross-checked against the local parquet when available).
"""

import os
import sys
import warnings
from pathlib import Path

warnings.filterwarnings("ignore")

import basedosdados as bd  # noqa: E402
import google.cloud.storage as gcs  # noqa: E402
import pyarrow.dataset as pads  # noqa: E402
from google.cloud import bigquery  # noqa: E402

BILLING_PROJECT = "basedosdados-dev"
DATASET_ID = "us_census_acs"
OUTPUT_ROOT = Path(
    os.environ.get("ACS_OUTPUT_ROOT", "/Users/rdahis/acs_data/output")
)

# Monkey-patch for requester-pays bucket
_orig_bucket = gcs.Client.bucket


def _patched_bucket(self, bucket_name, user_project=None):
    return _orig_bucket(self, bucket_name, user_project=BILLING_PROJECT)


gcs.Client.bucket = _patched_bucket

# smallest first (dicionario/variables tiny; microdata_person heaviest)
TABLES = [
    "dicionario",
    "variables",
    "data_profile_nation",
    "data_profile_congressional_district",
    "data_profile_cbsa",
    "data_profile_state",
    "data_profile_school_district",
    "data_profile_puma",
    "data_profile_county",
    "data_profile_place",
    "data_profile_zcta",
    "microdata_household",
    "microdata_person",
]


def local_rows(path: Path) -> int:
    try:
        return pads.dataset(
            str(path), format="parquet", partitioning="hive"
        ).count_rows()
    except Exception:
        return -1


def upload_table(slug: str) -> int:
    path = OUTPUT_ROOT / slug
    if not path.exists():
        raise FileNotFoundError(f"Missing output path: {path}")
    expected = local_rows(path)

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
        chunk_size=100
        * 1024
        * 1024,  # resumable 100MB chunks -> resilient to SSL drops on large files
    )

    client = bigquery.Client(project=BILLING_PROJECT)
    q = f"select count(*) as n from `{BILLING_PROJECT}.{DATASET_ID}_staging.{slug}`"
    n = next(iter(client.query(q).result())).n
    status = (
        "OK"
        if (expected < 0 or n == expected)
        else f"MISMATCH (local {expected:,})"
    )
    print(f"  {slug}: uploaded {n:,} rows — {status}", flush=True)
    if expected >= 0 and n != expected:
        raise ValueError(f"{slug}: BQ {n:,} != local {expected:,}")
    return n


def main():
    only = set(sys.argv[1:])
    tables = [s for s in TABLES if not only or s in only]
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
