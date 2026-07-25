"""Upload cleaned in_tcpd_elections parquet tables to BigQuery dev.

Uploads the TCPD Lok Dhaba India election tables (and the shared
br_bd_diretorios_in.state directory) to the basedosdados-dev staging area.

Usage:
    GOOGLE_APPLICATION_CREDENTIALS=~/.basedosdados/credentials/staging.json \
        .venv/bin/python models/in_tcpd_elections/code/upload.py [table_slug ...]

Uploads one table at a time (smallest first) and stops on the first failure.
Each table: delete stale staging prefix -> Table.create(replace) -> verify count.
"""

import sys
import warnings
from pathlib import Path

warnings.filterwarnings("ignore")

import basedosdados as bd  # noqa: E402
import google.cloud.storage as gcs  # noqa: E402
from google.cloud import bigquery  # noqa: E402

BILLING_PROJECT = "basedosdados-dev"
OUTPUT_ROOT = Path(__file__).resolve().parent.parent / "output"

# (dataset_id, table_id, expected_rows) — smallest first for fail-fast
TABLES = [
    ("in_tcpd_elections", "dictionary", 32),
    ("br_bd_diretorios_in", "state", 40),
    ("in_tcpd_elections", "general_elections", 91_669),
    ("in_tcpd_elections", "assembly_elections", 483_565),
]

# Monkey-patch for requester-pays bucket
_orig_bucket = gcs.Client.bucket


def _patched_bucket(self, bucket_name, user_project=None):
    return _orig_bucket(self, bucket_name, user_project=BILLING_PROJECT)


gcs.Client.bucket = _patched_bucket


def upload_table(dataset_id: str, table_id: str, expected_rows: int) -> int:
    path = OUTPUT_ROOT / table_id
    if not path.exists():
        raise FileNotFoundError(f"Missing output path: {path}")

    tb = bd.Table(dataset_id=dataset_id, table_id=table_id)

    # Delete stale GCS staging prefix before upload (avoids partition conflicts)
    st = bd.Storage(dataset_id=dataset_id, table_id=table_id)
    try:
        st.delete_table(mode="staging", not_found_ok=True)
        print("  staging prefix: cleared", flush=True)
    except Exception as e:
        print(f"  [warn] staging prefix cleanup: {e}", flush=True)

    tb.create(
        path=str(path),
        source_format="parquet",
        if_table_exists="replace",
        if_storage_data_exists="replace",
        if_dataset_exists="pass",
    )
    print("  create+upload: OK", flush=True)

    client = bigquery.Client(project=BILLING_PROJECT)
    q = (
        f"select count(*) as n from "
        f"`{BILLING_PROJECT}.{dataset_id}_staging.{table_id}`"
    )
    n = next(iter(client.query(q).result())).n

    status = "OK" if n == expected_rows else "ROW MISMATCH"
    print(
        f"  {dataset_id}_staging.{table_id}: {n:,} rows "
        f"(expected {expected_rows:,}) — {status}",
        flush=True,
    )
    if n != expected_rows:
        raise ValueError(
            f"{table_id}: row count {n:,} != expected {expected_rows:,}"
        )
    return n


def main():
    only = set(sys.argv[1:])
    tables = [t for t in TABLES if not only or t[1] in only]
    for dataset_id, table_id, expected in tables:
        print(f"=== {dataset_id}.{table_id} ===", flush=True)
        try:
            upload_table(dataset_id, table_id, expected)
        except Exception as e:
            print(f"  FAILED: {type(e).__name__}: {e}", flush=True)
            sys.exit(1)
    print("ALL TABLES UPLOADED", flush=True)


if __name__ == "__main__":
    main()
