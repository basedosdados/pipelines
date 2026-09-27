"""Upload the cleaned us_irs_form990 parquet to BigQuery staging (dev).

Streams every part file to ``gs://basedosdados-dev/staging/us_irs_form990/<table>/``
and issues a server-side ``load_table_from_uri`` (hive-partitioned on
``year=`` / ``extraction_date=``), instead of ``bd.Table.create``, which reads
the whole table into pandas first. A 0-row ``00_header.parquet`` sorts first in
every prefix so the table-approve CI step reads an empty file when it looks up
column names.

Prod data is never uploaded from here: the prod tables are materialised by the
table-approve action when the onboarding PR merges.

Usage (from the repo root, ``PYTHONPATH=.``)::

    GOOGLE_APPLICATION_CREDENTIALS=~/.basedosdados/credentials/staging.json \
      python models/us_irs_form990/code/upload.py [table ...]
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path

import google.cloud.storage as gcs
import pyarrow.parquet as pq
import requests
from google.api_core import exceptions as google_exceptions
from google.cloud import bigquery

BILLING_PROJECT = "basedosdados-dev"
BUCKET = "basedosdados-dev"
DATASET_ID = "us_irs_form990"
STAGING_DATASET = f"{DATASET_ID}_staging"
OUTPUT = (
    Path(
        os.environ.get(
            "FORM990_DATA_DIR", Path.home() / "Downloads/us_irs_form990_data"
        )
    )
    / "output"
)
CHUNK_SIZE = 64 * 1024 * 1024
UPLOAD_TIMEOUT = 600

# smallest first, so a credential or schema problem surfaces cheaply
TABLES = [
    "dicionario",
    "revocation",
    "organization",
    "return_financial",
    "compensation",
]
HIVE_PREFIX = {
    "organization": "extraction_date=",
    "return_financial": "year=",
    "compensation": "year=",
}

_orig_bucket = gcs.Client.bucket


def _patched_bucket(self, bucket_name, user_project=None):
    # The staging bucket is requester-pays.
    return _orig_bucket(self, bucket_name, user_project=BILLING_PROJECT)


gcs.Client.bucket = _patched_bucket


def staging_prefix(table: str) -> str:
    return f"staging/{DATASET_ID}/{table}"


def upload_blob(
    local: Path, blob_name: str, client: gcs.Client, attempts: int = 6
) -> None:
    last: Exception | None = None
    for attempt in range(attempts):
        try:
            blob = client.bucket(BUCKET).blob(blob_name)
            blob.chunk_size = CHUNK_SIZE
            blob.upload_from_filename(str(local), timeout=UPLOAD_TIMEOUT)
            return
        except (
            requests.exceptions.RequestException,
            google_exceptions.GoogleAPICallError,
            google_exceptions.RetryError,
            ConnectionError,
            TimeoutError,
            OSError,
        ) as exc:
            last = exc
            wait = min(120, 5 * 2**attempt)
            print(
                f"  upload {blob_name} failed ({type(exc).__name__}); retry in {wait}s",
                flush=True,
            )
            time.sleep(wait)
    raise RuntimeError(f"giving up on {blob_name}") from last


def clear_prefix(table: str, client: gcs.Client) -> int:
    bucket = client.bucket(BUCKET)
    blobs = list(client.list_blobs(bucket, prefix=staging_prefix(table) + "/"))
    for b in blobs:
        b.delete()
    return len(blobs)


def upload_table(table: str, client: gcs.Client) -> tuple[int, int]:
    base = OUTPUT / table
    files = sorted(p for p in base.rglob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"no parquet under {base}")
    rows, n = 0, 0
    for p in files:
        rel = p.relative_to(base).as_posix()
        rows += pq.ParquetFile(p).metadata.num_rows
        upload_blob(p, f"{staging_prefix(table)}/{rel}", client)
        n += 1
        if n % 25 == 0:
            print(f"  {n}/{len(files)} files", flush=True)
    return rows, n


def load_staging_table(table: str) -> int:
    bq = bigquery.Client(project=BILLING_PROJECT)
    ds = bigquery.Dataset(f"{BILLING_PROJECT}.{STAGING_DATASET}")
    ds.location = "US"
    bq.create_dataset(ds, exists_ok=True)
    target = f"{BILLING_PROJECT}.{STAGING_DATASET}.{table}"
    bq.query(f"drop table if exists `{target}`").result()
    config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition="WRITE_TRUNCATE",
    )
    if table in HIVE_PREFIX:
        hp = bigquery.HivePartitioningOptions()
        hp.mode = "STRINGS"
        hp.source_uri_prefix = f"gs://{BUCKET}/{staging_prefix(table)}/"
        config.hive_partitioning = hp
    job = bq.load_table_from_uri(
        f"gs://{BUCKET}/{staging_prefix(table)}/*.parquet",
        target,
        job_config=config,
    )
    job.result()
    return bq.get_table(target).num_rows


def main() -> None:
    only = set(sys.argv[1:])
    tables = [t for t in TABLES if not only or t in only]
    client = gcs.Client(project=BILLING_PROJECT)
    for table in tables:
        t0 = time.time()
        print(f"=== {table} ===", flush=True)
        n = clear_prefix(table, client)
        print(f"  cleared {n} blob(s)")
        rows, files = upload_table(table, client)
        print(f"  uploaded {files} files, {rows:,} rows", flush=True)
        loaded = load_staging_table(table)
        print(
            f"  staging rows: {loaded:,}  ({time.time() - t0:,.0f}s)",
            flush=True,
        )
        if loaded != rows:
            raise SystemExit(f"{table}: loaded {loaded} != local {rows}")
    print("ALL TABLES UPLOADED")


if __name__ == "__main__":
    main()
