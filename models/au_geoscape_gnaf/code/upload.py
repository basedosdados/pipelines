"""Upload cleaned G-NAF parquet to BigQuery dev (basedosdados-dev).

Dataset:         au_geoscape_gnaf
Staging dataset: au_geoscape_gnaf_staging

Strategy
--------
The cleaned parquet is hive-partitioned on ``snapshot_date`` (DATE, 2026-05-01)
and ``id_state`` (STRING, 1-9), and those two columns are encoded ONLY in the
GCS path, not stored in the files. To turn them into STRING columns in the
staging table we:

  1. stream each ``data.parquet`` to
     ``gs://basedosdados-dev/staging/au_geoscape_gnaf/<table>/...`` with
     ``blob.upload_from_filename`` (no pandas -> no RAM blowup on the 16.9M-row
     address_detail table; see memory ``reference_bd_table_create_ram_blowup``);
  2. create the staging table server-side with
     ``bigquery.Client.load_table_from_uri`` using ``source_format=PARQUET``,
     ``write_disposition=WRITE_TRUNCATE`` and, for the partitioned tables,
     ``HivePartitioningOptions(mode="STRINGS", source_uri_prefix=...)`` so
     ``snapshot_date`` and ``id_state`` are appended as STRING columns.

``dicionario`` is a single unpartitioned parquet -> plain load, no hive options.

Dev only. Never touches prod (basedosdados).
"""

import os
import sys
from pathlib import Path

# --- Credentials: basedosdados-dev (staging.json profile) ---
_DEFAULT_CREDS = str(
    Path.home() / ".basedosdados" / "credentials" / "staging.json"
)
os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS", _DEFAULT_CREDS)

import google.cloud.storage as gcs  # noqa: E402
from google.cloud import bigquery  # noqa: E402

BILLING_PROJECT = "basedosdados-dev"
BUCKET = "basedosdados-dev"
DATASET_ID = "au_geoscape_gnaf"
STAGING_DATASET = f"{DATASET_ID}_staging"
OUTPUT_ROOT = Path.home() / "Downloads" / "au_geoscape_gnaf_data" / "output"

# ---- Monkey-patch for requester-pays bucket ----
_orig_bucket = gcs.Client.bucket


def _patched_bucket(self, bucket_name, user_project=None):
    return _orig_bucket(self, bucket_name, user_project=BILLING_PROJECT)


gcs.Client.bucket = _patched_bucket

# table_slug -> (expected_row_count, is_hive_partitioned)
TABLES = {
    "address_detail": (16_905_824, True),
    "street_locality": (765_407, True),
    "locality": (17_578, True),
    "dicionario": (434, False),
}


def _storage_client():
    return gcs.Client(project=BILLING_PROJECT)


def _bq_client():
    return bigquery.Client(project=BILLING_PROJECT)


def ensure_staging_dataset():
    """Create the staging dataset (location US) if it does not exist."""
    bq = _bq_client()
    ds_id = f"{BILLING_PROJECT}.{STAGING_DATASET}"
    try:
        bq.get_dataset(ds_id)
    except Exception:
        ds = bigquery.Dataset(ds_id)
        ds.location = "US"
        bq.create_dataset(ds, exists_ok=True)
        print(f"created dataset {ds_id} (US)", flush=True)


def clear_prefix(prefix):
    """Delete every blob under a GCS prefix (clears stale staging data)."""
    client = _storage_client()
    bucket = client.bucket(BUCKET)
    blobs = list(client.list_blobs(bucket, prefix=prefix))
    for b in blobs:
        b.delete()
    print(
        f"cleared {len(blobs)} stale blob(s) under gs://{BUCKET}/{prefix}",
        flush=True,
    )


def stream_upload(local_path: Path, blob_path: str):
    """Upload one local parquet file to GCS without loading it into memory."""
    client = _storage_client()
    bucket = client.bucket(BUCKET)
    blob = bucket.blob(blob_path)
    blob.upload_from_filename(str(local_path))
    size_mb = local_path.stat().st_size / 1e6
    print(f"  uploaded {blob_path} ({size_mb:.1f} MB)", flush=True)


def load_staging(table_slug: str, staging_prefix: str, hive: bool):
    """Create the staging BQ table from the uploaded GCS parquet."""
    ensure_staging_dataset()
    bq = _bq_client()
    dest = f"{BILLING_PROJECT}.{STAGING_DATASET}.{table_slug}"
    # staging_prefix ends with "/"; for hive loads the prefix must NOT carry a
    # trailing slash and the wildcard must span the partition dirs ("/*").
    base = f"gs://{BUCKET}/{staging_prefix}".rstrip("/")

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    if hive:
        source_uri = f"{base}/*"
        hp = bigquery.HivePartitioningOptions()
        hp.mode = "STRINGS"
        hp.source_uri_prefix = base
        job_config.hive_partitioning = hp
    else:
        source_uri = f"{base}/*.parquet"

    print(f"  loading {source_uri} -> {dest}", flush=True)
    job = bq.load_table_from_uri(source_uri, dest, job_config=job_config)
    job.result()
    print(f"  load job {job.job_id} done", flush=True)


def count_rows(table_slug: str) -> int:
    bq = _bq_client()
    fqn = f"{BILLING_PROJECT}.{STAGING_DATASET}.{table_slug}"
    job = bq.query(f"SELECT COUNT(*) AS n FROM `{fqn}`")
    for row in job.result():
        return row.n
    return None


def check_partitions(table_slug: str):
    """For hive tables: confirm snapshot_date + id_state exist and cover 9 states."""
    bq = _bq_client()
    fqn = f"{BILLING_PROJECT}.{STAGING_DATASET}.{table_slug}"
    q = (
        f"SELECT snapshot_date, id_state, COUNT(*) AS n "
        f"FROM `{fqn}` GROUP BY 1, 2 ORDER BY id_state"
    )
    rows = list(bq.query(q).result())
    print(f"  partition groups ({len(rows)}):", flush=True)
    for r in rows:
        print(
            f"    snapshot_date={r.snapshot_date} id_state={r.id_state} n={r.n}",
            flush=True,
        )
    n_states = len({r.id_state for r in rows})
    snaps = {str(r.snapshot_date) for r in rows}
    assert n_states == 9, f"expected 9 id_state groups, got {n_states}"
    assert snaps == {"2026-05-01"}, f"unexpected snapshot_date values: {snaps}"
    print(
        "  partition columns OK (9 states, snapshot_date=2026-05-01)",
        flush=True,
    )


def upload_partitioned(table_slug: str, expected: int):
    print(f"\n=== {table_slug} (hive-partitioned) ===", flush=True)
    staging_prefix = f"staging/{DATASET_ID}/{table_slug}/"

    # 1. Clear stale staging prefix
    clear_prefix(staging_prefix)

    # 2. Stream each state's parquet to GCS, preserving the hive path
    table_root = OUTPUT_ROOT / table_slug
    files = sorted(table_root.rglob("data.parquet"))
    if not files:
        raise FileNotFoundError(f"no data.parquet under {table_root}")
    print(f"  {len(files)} partition file(s) to upload", flush=True)
    for f in files:
        rel = f.relative_to(
            table_root
        )  # snapshot_date=.../id_state=.../data.parquet
        blob_path = f"{staging_prefix}{rel.as_posix()}"
        stream_upload(f, blob_path)

    # 3. Create staging table server-side with hive partitioning
    load_staging(table_slug, staging_prefix, hive=True)

    # 4. Verify
    n = count_rows(table_slug)
    print(f"  row count: {n} (expected {expected})", flush=True)
    if n != expected:
        raise AssertionError(
            f"ROW COUNT MISMATCH {table_slug}: got {n}, expected {expected}"
        )
    check_partitions(table_slug)
    print(f"OK {table_slug}: {n} rows", flush=True)
    return n


def upload_flat(table_slug: str, expected: int):
    print(f"\n=== {table_slug} (single file) ===", flush=True)
    staging_prefix = f"staging/{DATASET_ID}/{table_slug}/"
    local = OUTPUT_ROOT / table_slug / "data.parquet"
    if not local.exists():
        raise FileNotFoundError(f"missing parquet: {local}")

    clear_prefix(staging_prefix)
    stream_upload(local, f"{staging_prefix}data.parquet")
    load_staging(table_slug, staging_prefix, hive=False)

    n = count_rows(table_slug)
    print(f"  row count: {n} (expected {expected})", flush=True)
    if n != expected:
        raise AssertionError(
            f"ROW COUNT MISMATCH {table_slug}: got {n}, expected {expected}"
        )
    print(f"OK {table_slug}: {n} rows", flush=True)
    return n


def upload_one(table_slug: str):
    expected, hive = TABLES[table_slug]
    if hive:
        return upload_partitioned(table_slug, expected)
    return upload_flat(table_slug, expected)


def main():
    only = sys.argv[1] if len(sys.argv) > 1 else None
    results = {}
    for slug in TABLES:
        if only and slug != only:
            continue
        results[slug] = upload_one(slug)
    print("\n=== DONE ===", flush=True)
    for slug, n in results.items():
        print(f"  {slug}: {n} rows", flush=True)


if __name__ == "__main__":
    main()
