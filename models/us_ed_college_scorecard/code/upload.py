#!/usr/bin/env python3
"""
Upload the cleaned College Scorecard parquet to BigQuery dev.

Uploads one table at a time, smallest first, and verifies the row count in
BigQuery against the count written by clean_data.py. Stops on the first
mismatch rather than continuing with a half-loaded dataset.

Usage:
    uv run python models/us_ed_college_scorecard/code/upload.py [table_slug ...]
"""

import json
import os
import pathlib
import sys
import warnings

warnings.filterwarnings("ignore")

import basedosdados as bd  # noqa: E402
import google.cloud.storage as gcs  # noqa: E402
import pyarrow as pa  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402
from google.cloud import bigquery  # noqa: E402

BILLING_PROJECT = "basedosdados-dev"
DATASET_ID = "us_ed_college_scorecard"
CODE_DIR = pathlib.Path(__file__).resolve().parent
OUTPUT_ROOT = pathlib.Path(
    os.environ.get(
        "OUTPUT_DIR",
        pathlib.Path.home() / "Downloads/us_ed_college_scorecard_data/output",
    )
)

# The bucket is requester-pays, so every bucket handle needs a billing project.
_orig_bucket = gcs.Client.bucket


def _patched_bucket(self, bucket_name, user_project=None):
    return _orig_bucket(self, bucket_name, user_project=BILLING_PROJECT)


gcs.Client.bucket = _patched_bucket


def write_zero_row_header(table_dir):
    """Prepend a 0-row parquet so table-approve reads a tiny first file.

    The prod table-approve job builds its schema from the FIRST parquet under
    the staging prefix and loads it into memory; on a multi-GB first partition
    that OOMs the runner. A 0-row file sorting before every partition
    directory fixes it and costs nothing.
    """
    partitions = sorted(p for p in table_dir.glob("year=*/data.parquet"))
    if not partitions:
        return
    header = table_dir / "00_header.parquet"
    schema = pq.read_schema(partitions[0])
    pq.write_table(
        pa.Table.from_pylist([], schema=schema), header, compression="snappy"
    )


def upload_table(slug, expected_rows):
    path = OUTPUT_ROOT / slug
    if not path.exists():
        raise FileNotFoundError(f"missing output path: {path}")
    write_zero_row_header(path)

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
    query = f"select count(*) as n from `{BILLING_PROJECT}.{DATASET_ID}_staging.{slug}`"
    loaded = next(iter(client.query(query).result())).n
    ok = loaded == expected_rows
    print(
        f"  {slug}: {loaded:,} rows in BigQuery (expected {expected_rows:,}) — "
        f"{'OK' if ok else 'ROW MISMATCH'}"
    )
    if not ok:
        raise ValueError(f"{slug}: {loaded:,} != {expected_rows:,}")
    return loaded


def main():
    stats = json.loads((CODE_DIR / "clean_stats.json").read_text())
    tables = sorted(stats["rows"].items(), key=lambda kv: kv[1])
    only = set(sys.argv[1:])
    for slug, expected in tables:
        if only and slug not in only:
            continue
        print(f"=== {slug} ===", flush=True)
        try:
            upload_table(slug, expected)
        except Exception as exc:
            print(f"  FAILED: {type(exc).__name__}: {exc}")
            sys.exit(1)
    print("ALL TABLES UPLOADED")


if __name__ == "__main__":
    main()
