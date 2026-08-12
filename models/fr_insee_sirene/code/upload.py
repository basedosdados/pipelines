import sys
from pathlib import Path

import google.cloud.storage as gcs
from basedosdados import Storage, Table
from google.cloud import bigquery

BILLING_PROJECT = "basedosdados-dev"
DATASET_ID = "fr_insee_sirene"
STAGING_DATASET = f"{DATASET_ID}_staging"
OUTPUT_ROOT = Path.home() / "Downloads" / "fr_insee_sirene_data" / "output"

# ---- Monkey-patch for requester-pays bucket ----
_orig_bucket = gcs.Client.bucket


def _patched_bucket(self, bucket_name, user_project=None):
    return _orig_bucket(self, bucket_name, user_project=BILLING_PROJECT)


gcs.Client.bucket = _patched_bucket

# table_slug -> expected row count
TABLES = {
    "dicionario": 163,
    "unite_legale": 29922486,
    "unite_legale_historico": 71355318,
    "etablissement": 43896818,
    "etablissement_historico": 95865102,
}


def count_rows(table_slug):
    client = bigquery.Client(project=BILLING_PROJECT)
    fqn = f"{BILLING_PROJECT}.{STAGING_DATASET}.{table_slug}"
    q = f"SELECT COUNT(*) AS n FROM `{fqn}`"
    job = client.query(q)
    for row in job.result():
        return row.n
    return None


def upload_one(table_slug, expected):
    path = OUTPUT_ROOT / table_slug / "data.parquet"
    if not path.exists():
        raise FileNotFoundError(f"Missing parquet: {path}")

    print(f"\n=== {table_slug} ===", flush=True)
    print(f"path: {path}", flush=True)

    # 1. Clear stale staging prefix
    st = Storage(dataset_id=DATASET_ID, table_id=table_slug)
    st.delete_table(mode="staging", not_found_ok=True)
    print("staging prefix cleared", flush=True)

    # 2. Create staging table + upload parquet to GCS (bd 2.0.3: create does both)
    tb = Table(dataset_id=DATASET_ID, table_id=table_slug)
    tb.create(
        path=str(path),
        source_format="parquet",
        if_table_exists="replace",
        if_storage_data_exists="replace",
    )
    print("staging table created + parquet uploaded", flush=True)

    # 3. Verify row count
    n = count_rows(table_slug)
    print(f"row count: {n} (expected {expected})", flush=True)
    if n != expected:
        raise AssertionError(
            f"ROW COUNT MISMATCH for {table_slug}: got {n}, expected {expected}"
        )
    print(f"OK {table_slug}: {n} rows", flush=True)
    return n


def main():
    only = sys.argv[1] if len(sys.argv) > 1 else None
    results = {}
    for slug, expected in TABLES.items():
        if only and slug != only:
            continue
        n = upload_one(slug, expected)
        results[slug] = n
    print("\n=== DONE ===", flush=True)
    for slug, n in results.items():
        print(f"  {slug}: {n} rows", flush=True)


if __name__ == "__main__":
    main()
