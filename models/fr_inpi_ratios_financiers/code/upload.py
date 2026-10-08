"""Upload cleaned fr_inpi_ratios_financiers parquet to the BigQuery dev staging dataset.

Usage:
    .venv/bin/python models/fr_inpi_ratios_financiers/code/upload.py [table_slug ...]

Targets ``basedosdados-dev`` only (prod tables are materialised by the merge).
Reads ``$FR_INPI_RATIOS_DATA_DIR/output/<table>/`` and the row counts recorded by
``clean.py`` in ``output/_manifest.json``; stops on the first table whose
staging row count does not match.
"""

import json
import os
import sys
from pathlib import Path

import basedosdados as bd
import google.cloud.storage as gcs
from google.cloud import bigquery

BILLING_PROJECT = "basedosdados-dev"
DATASET_ID = "fr_inpi_ratios_financiers"
OUTPUT_ROOT = (
    Path(
        os.environ.get(
            "FR_INPI_RATIOS_DATA_DIR",
            Path.home() / "bd_scratch" / "fr_inpi_ratios_financiers_data",
        )
    )
    / "output"
)
TABLES = ["dicionario", "ratios_financiers"]

# Monkey-patch for the requester-pays bucket.
_orig_bucket = gcs.Client.bucket


def _patched_bucket(self, bucket_name, user_project=None):
    return _orig_bucket(self, bucket_name, user_project=BILLING_PROJECT)


gcs.Client.bucket = _patched_bucket


def upload_table(slug: str, expected: int) -> int:
    path = OUTPUT_ROOT / slug
    if not path.exists():
        raise FileNotFoundError(f"Missing output for {slug}: {path}")

    st = bd.Storage(dataset_id=DATASET_ID, table_id=slug)
    st.delete_table(mode="staging", not_found_ok=True)

    bd.Table(dataset_id=DATASET_ID, table_id=slug).create(
        path=str(path),
        source_format="parquet",
        if_table_exists="replace",
        if_storage_data_exists="replace",
        if_dataset_exists="pass",
    )

    client = bigquery.Client(project=BILLING_PROJECT)
    q = f"select count(*) as n from `{BILLING_PROJECT}.{DATASET_ID}_staging.{slug}`"
    n = next(iter(client.query(q).result())).n
    print(f"  {slug}: staging {n:,} rows (expected {expected:,})", flush=True)
    if n != expected:
        raise ValueError(f"{slug}: row count {n:,} != expected {expected:,}")
    return n


def main() -> None:
    manifest = json.loads((OUTPUT_ROOT / "_manifest.json").read_text())
    only = sys.argv[1:]
    for slug in [t for t in TABLES if not only or t in only]:
        print(f"=== {slug} ===", flush=True)
        upload_table(slug, manifest[slug])
    print("ALL TABLES UPLOADED", flush=True)


if __name__ == "__main__":
    main()
