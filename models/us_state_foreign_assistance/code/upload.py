"""Upload the cleaned us_state_foreign_assistance Parquet to BigQuery (dev).

Delegates to the same ``pipelines.utils.tasks._upload_to_gcs`` helper the
recurring flow uses, so the one-shot bootstrap and the pipeline produce an
identical staging table. That matters more than it looks: the helper creates an
**external** table over ``gs://<bucket>/staging/<ds>/<table>/*``, whereas a
BigQuery load job would create a *native* one. A native staging table ignores
the files a later pipeline run uploads, so dbt would keep reading the bootstrap
data forever, with nothing failing to signal it.

RAM stays flat because the table is created from a 0-row header file (see
``dump_header``); the data itself is streamed to GCS by ``Storage.upload``,
never loaded into pandas.

Each release of the source restates the whole history, so every run is a full
replace: the staging prefix is cleared before the upload.

Reads ``<data dir>/output/<table>/*.parquet`` (default data dir
``~/Downloads/us_state_foreign_assistance_data``, override with
US_STATE_FOREIGN_ASSISTANCE_DATA). Requires GOOGLE_APPLICATION_CREDENTIALS
pointing at a service account with write access to ``basedosdados-dev``.
"""

import argparse
import os
import sys
from pathlib import Path

import google.cloud.storage as gcs
from google.cloud import bigquery

from pipelines.utils.tasks import _upload_to_gcs

PROJECT = "basedosdados-dev"
BUCKET = "basedosdados-dev"
DATASET_ID = "us_state_foreign_assistance"
DATA_DIR = Path(
    os.environ.get(
        "US_STATE_FOREIGN_ASSISTANCE_DATA",
        str(Path.home() / "Downloads" / "us_state_foreign_assistance_data"),
    )
)
OUTPUT_DIR = DATA_DIR / "output"
TABLES = ["transaction", "budget", "dicionario"]

# The bucket is requester-pays; bill every request to the dev project.
_orig_bucket = gcs.Client.bucket


def _patched_bucket(self, bucket_name, user_project=None):
    return _orig_bucket(self, bucket_name, user_project=PROJECT)


gcs.Client.bucket = _patched_bucket


def clear_staging_prefix(table: str) -> int:
    client = gcs.Client(project=PROJECT)
    bucket = client.bucket(BUCKET)
    prefix = f"staging/{DATASET_ID}/{table}/"
    blobs = list(client.list_blobs(bucket, prefix=prefix))
    for blob in blobs:
        blob.delete()
    return len(blobs)


def upload_table(table: str, expected: int | None) -> None:
    path = OUTPUT_DIR / table
    if not path.is_dir():
        raise FileNotFoundError(f"no output directory at {path}")
    print(f"--- {table} ---", flush=True)

    bq = bigquery.Client(project=PROJECT)
    ds = bigquery.Dataset(f"{PROJECT}.{DATASET_ID}_staging")
    ds.location = "US"
    bq.create_dataset(ds, exists_ok=True)

    # A native table left by an earlier load job would shadow the GCS files the
    # external definition is meant to read; drop it so the helper recreates it.
    ref = f"{PROJECT}.{DATASET_ID}_staging.{table}"
    try:
        if bq.get_table(ref).table_type != "EXTERNAL":
            bq.query(f"drop table `{ref}`").result()
            print("dropped stale native staging table", flush=True)
    except Exception:
        pass

    print(f"cleared {clear_staging_prefix(table)} stale objects", flush=True)
    _upload_to_gcs(
        data_path=path,
        dataset_id=DATASET_ID,
        table_id=table,
        bucket_name=BUCKET,
        dump_mode="append",
        source_format="parquet",
    )

    tbl = bq.get_table(ref)
    n = next(iter(bq.query(f"select count(*) as n from `{ref}`").result()))[
        "n"
    ]
    print(f"{tbl.table_type} staging table: {n:,} rows", flush=True)
    if expected is not None and n != expected:
        raise RuntimeError(f"{table}: row count {n} != expected {expected}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", default=None)
    parser.add_argument(
        "--expected",
        default=None,
        help="comma-separated table=rows pairs to assert after loading",
    )
    args = parser.parse_args()
    expected = {}
    if args.expected:
        for pair in args.expected.split(","):
            k, v = pair.split("=")
            expected[k.strip()] = int(v)
    for table in [args.table] if args.table else TABLES:
        try:
            upload_table(table, expected.get(table))
        except Exception as e:
            print(f"FAILED: {table}: {e}", flush=True)
            sys.exit(1)
    print("ALL DONE", flush=True)


if __name__ == "__main__":
    main()
