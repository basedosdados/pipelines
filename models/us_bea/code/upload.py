"""One-shot onboarding upload of us_bea cleaned parquet to BigQuery dev staging.

Uploads typed parquet (year INT64 via hive path, value FLOAT64, rest STRING) to
`basedosdados-dev.us_bea_staging.<table>`. Uses server-side load_table_from_uri
(native tables) for every table to avoid the bd.Table all-STRING/pandas RAM path.

Billing/requester-pays project: basedosdados-dev. Auth: ADC.
"""

import argparse
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

import google.cloud.storage as gcs
from google.cloud import bigquery

BILLING_PROJECT = "basedosdados-dev"
BUCKET = "basedosdados-dev"
GCP_DATASET_ID = "us_bea"
STAGING_DATASET = "us_bea_staging"
DATA_ROOT = os.environ.get(
    "US_BEA_DATA", os.path.expanduser("~/Downloads/us_bea_data/output")
)

# Monkey-patch for requester-pays bucket
_orig_bucket = gcs.Client.bucket


def _patched_bucket(self, bucket_name, user_project=None):
    return _orig_bucket(self, bucket_name, user_project=BILLING_PROJECT)


gcs.Client.bucket = _patched_bucket

# name -> partitioned (hive year=YYYY) or single flat file
TABLES = [
    ("nipa", True, 2_187_327),
    ("gdp_by_industry", True, 348_824),
    ("regional_state", True, 7_854_588),
    ("regional_county", True, 49_703_844),
    ("regional_metro", True, 39_474),
    ("dicionario", False, 506),
]

storage_client = gcs.Client(project=BILLING_PROJECT)
bq = bigquery.Client(project=BILLING_PROJECT)


def ensure_dataset():
    ds_ref = bigquery.Dataset(f"{BILLING_PROJECT}.{STAGING_DATASET}")
    ds_ref.location = "US"
    bq.create_dataset(ds_ref, exists_ok=True)
    print(f"  dataset {BILLING_PROJECT}.{STAGING_DATASET} ready")


def delete_prefix(prefix):
    bucket = storage_client.bucket(BUCKET)
    blobs = list(storage_client.list_blobs(bucket, prefix=prefix))
    if not blobs:
        print(f"  no stale blobs at {prefix}")
        return
    for i in range(0, len(blobs), 500):
        bucket.delete_blobs(blobs[i : i + 500])
    print(f"  deleted {len(blobs)} stale blobs at {prefix}")


def local_files(name):
    root = os.path.join(DATA_ROOT, name)
    files = []
    for dirpath, _, filenames in os.walk(root):
        for fn in filenames:
            if fn.endswith(".parquet"):
                full = os.path.join(dirpath, fn)
                rel = os.path.relpath(full, root)
                files.append((full, rel))
    return files


def _upload_one(bucket, prefix, full, rel):
    blob = bucket.blob(prefix + rel.replace(os.sep, "/"))
    blob.chunk_size = 256 * 1024 * 1024
    for attempt in range(3):
        try:
            blob.upload_from_filename(full)
            return
        except Exception:
            if attempt == 2:
                raise


def upload_files(name, prefix):
    bucket = storage_client.bucket(BUCKET)
    files = local_files(name)
    total = len(files)
    print(f"  uploading {total} parquet files -> gs://{BUCKET}/{prefix}")
    done = 0
    with ThreadPoolExecutor(max_workers=32) as ex:
        futs = {
            ex.submit(_upload_one, bucket, prefix, full, rel): rel
            for full, rel in files
        }
        for fut in as_completed(futs):
            fut.result()
            done += 1
            if done % 2000 == 0 or done == total:
                print(f"    {done}/{total}")
    return total


def load_table(name, partitioned, prefix):
    table_id = f"{BILLING_PROJECT}.{STAGING_DATASET}.{name}"
    uri = f"gs://{BUCKET}/{prefix}*.parquet"
    jc = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    if partitioned:
        hp = bigquery.HivePartitioningOptions()
        hp.mode = "AUTO"
        hp.source_uri_prefix = f"gs://{BUCKET}/{prefix}"
        jc.hive_partitioning = hp
    job = bq.load_table_from_uri(uri, table_id, job_config=jc)
    job.result()
    print(f"  load job {job.job_id} done, output_rows={job.output_rows}")
    return table_id


def count_rows(table_id):
    q = f"SELECT COUNT(*) AS n FROM `{table_id}`"
    return next(iter(bq.query(q).result())).n


def process(name, partitioned, expected):
    print(f"\n=== {name} (expected {expected:,}) ===")
    prefix = f"staging/{GCP_DATASET_ID}/{name}/"
    delete_prefix(prefix)
    upload_files(name, prefix)
    table_id = load_table(name, partitioned, prefix)
    n = count_rows(table_id)
    ok = n == expected
    print(f"  COUNT(*)={n:,} expected={expected:,} match={'Y' if ok else 'N'}")
    if not ok:
        print(
            f"!! ROW COUNT MISMATCH for {name}: got {n}, expected {expected}"
        )
        sys.exit(1)
    return name, n, prefix, table_id


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", default=None, help="comma-separated table names")
    args = ap.parse_args()

    ensure_dataset()
    only = set(args.only.split(",")) if args.only else None
    results = []
    for name, partitioned, expected in TABLES:
        if only and name not in only:
            continue
        results.append(process(name, partitioned, expected))

    print("\n=== SUMMARY ===")
    for name, n, prefix, table_id in results:
        print(f"  {name}: {n:,} rows -> {table_id}  (gs://{BUCKET}/{prefix})")


if __name__ == "__main__":
    main()
