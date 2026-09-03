"""Upload the cleaned us_state_foreign_assistance Parquet to BigQuery (dev).

Memory-safe path for a 4M-row table: stream each file to GCS with a chunked
resumable upload, then run a server-side BigQuery load job. Nothing is read
into pandas. The files are all-STRING, so the staging table is all-STRING,
matching what the recurring pipeline's ``upload_to_gcs`` would create.

Reads ``<data dir>/output/<table>/*.parquet`` (default data dir
``~/Downloads/us_state_foreign_assistance_data``, override with
US_STATE_FOREIGN_ASSISTANCE_DATA). Requires GOOGLE_APPLICATION_CREDENTIALS
pointing at a service account with write access to ``basedosdados-dev``.
"""

import argparse
import os
import sys
from pathlib import Path

from google.cloud import bigquery, storage

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


def upload_table(table: str, expected: int | None) -> None:
    local = OUTPUT_DIR / table
    files = sorted(local.glob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"no parquet under {local}")
    print(f"--- {table}: {len(files)} files ---", flush=True)

    gcs = storage.Client(project=PROJECT)
    bucket = gcs.bucket(BUCKET, user_project=PROJECT)
    prefix = f"staging/{DATASET_ID}/{table}/"
    stale = list(gcs.list_blobs(bucket, prefix=prefix))
    for b in stale:
        b.delete()
    print(
        f"deleted {len(stale)} stale objects under gs://{BUCKET}/{prefix}",
        flush=True,
    )
    for f in files:
        blob = bucket.blob(prefix + f.name)
        blob.chunk_size = 64 * 1024 * 1024
        blob.upload_from_filename(str(f))
    print("gcs upload: OK", flush=True)

    bq = bigquery.Client(project=PROJECT)
    ds_ref = bigquery.Dataset(f"{PROJECT}.{DATASET_ID}_staging")
    ds_ref.location = "US"
    bq.create_dataset(ds_ref, exists_ok=True)
    table_ref = f"{PROJECT}.{DATASET_ID}_staging.{table}"
    bq.query(f"drop table if exists `{table_ref}`").result()
    job = bq.load_table_from_uri(
        f"gs://{BUCKET}/{prefix}*.parquet",
        table_ref,
        job_config=bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        ),
    )
    job.result()
    n = bq.get_table(table_ref).num_rows
    print(f"bq load: {n:,} rows", flush=True)
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
