"""Upload the cleaned us_ed_nces_ccd Parquet tables to BigQuery dev.

Target is always `basedosdados-dev`; the production tables are materialised by
the table-approve action when the onboarding PR merges, never from here.

Usage:
    uv run python models/us_ed_nces_ccd/code/upload.py [table_slug ...]

Uploads smallest first and stops on the first failure, so a broken table is
never followed by more uploads on top of it.
"""

from __future__ import annotations

import json
import os
import sys
import warnings
from pathlib import Path

warnings.filterwarnings("ignore")

import basedosdados as bd  # noqa: E402
import google.cloud.storage as gcs  # noqa: E402
from google.cloud import bigquery  # noqa: E402

BILLING_PROJECT = "basedosdados-dev"
DATASET_ID = "us_ed_nces_ccd"
DATA_DIR = Path(
    os.environ.get(
        "CCD_DATA_DIR", Path.home() / "Downloads" / "us_ed_nces_ccd_data"
    )
)
OUTPUT_ROOT = DATA_DIR / "output"

# The staging bucket is requester-pays, so every bucket handle needs a billing
# project attached.
_orig_bucket = gcs.Client.bucket


def _patched_bucket(self, bucket_name, user_project=None):
    return _orig_bucket(self, bucket_name, user_project=BILLING_PROJECT)


gcs.Client.bucket = _patched_bucket

#: Smallest first, so a schema problem surfaces on a cheap table.
TABLE_ORDER = [
    "dicionario",
    "school_district",
    "district_finance",
    "school",
    "staff",
    "school_enrollment",
]


def expected_rows() -> dict[str, int]:
    path = DATA_DIR / "row_counts.json"
    return json.loads(path.read_text()) if path.exists() else {}


def upload_table(slug: str, expected: int | None) -> int:
    path = OUTPUT_ROOT / slug
    if not path.exists():
        raise FileNotFoundError(f"missing output path: {path}")

    # A stale staging prefix from an earlier run makes BigQuery reject the
    # external table on conflicting partition keys, so it goes first.
    storage = bd.Storage(dataset_id=DATASET_ID, table_id=slug)
    try:
        storage.delete_table(mode="staging", not_found_ok=True)
    except Exception as exc:
        print(f"  [warn] staging prefix cleanup: {exc}")

    table = bd.Table(dataset_id=DATASET_ID, table_id=slug)
    table.create(
        path=str(path),
        source_format="parquet",
        if_table_exists="replace",
        if_storage_data_exists="replace",
        if_dataset_exists="pass",
    )

    client = bigquery.Client(project=BILLING_PROJECT)
    query = f"select count(*) as n from `{BILLING_PROJECT}.{DATASET_ID}_staging.{slug}`"
    n = next(iter(client.query(query).result())).n

    if expected is None:
        print(f"  {slug}: uploaded {n:,} rows")
    elif n == expected:
        print(f"  {slug}: uploaded {n:,} rows — OK")
    else:
        raise ValueError(f"{slug}: uploaded {n:,} rows, expected {expected:,}")
    return n


def main() -> None:
    only = set(sys.argv[1:])
    expected = expected_rows()
    for slug in TABLE_ORDER:
        if only and slug not in only:
            continue
        print(f"=== {slug} ===", flush=True)
        try:
            upload_table(slug, expected.get(slug))
        except Exception as exc:
            print(f"  FAILED: {type(exc).__name__}: {exc}")
            sys.exit(1)
    print("ALL TABLES UPLOADED")


if __name__ == "__main__":
    main()
