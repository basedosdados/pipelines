"""Upload the CPS parquet tables to BigQuery dev (basedosdados-dev).

The parquet lives outside the repo, at ~/cps_build/parquet (the Phase-A golden
fixture), because the raw + intermediate Stata tree is far too large for Dropbox.

Usage:
    GOOGLE_APPLICATION_CREDENTIALS=~/.basedosdados/credentials/staging.json \
        .venv/bin/python models/us_census_cps/code/upload.py [table_slug ...]

Uploads sequentially (smallest first). Stops on first failure.
"""

import os
import sys
import warnings
from pathlib import Path

warnings.filterwarnings("ignore")

import basedosdados as bd  # noqa: E402
import google.cloud.storage as gcs  # noqa: E402
from google.cloud import bigquery  # noqa: E402

BILLING_PROJECT = "basedosdados-dev"
DATASET_ID = "us_census_cps"
PARQUET_ROOT = Path(
    os.environ.get("CPS_PARQUET", Path.home() / "cps_build" / "parquet")
)

# Monkey-patch for requester-pays bucket
_orig_bucket = gcs.Client.bucket


def _patched_bucket(self, bucket_name, user_project=None):
    return _orig_bucket(self, bucket_name, user_project=BILLING_PROJECT)


gcs.Client.bucket = _patched_bucket

# (table_slug, expected_rows) — smallest first. Row counts are the Phase-A totals.
TABLES = [
    ("dictionary", None),
    ("march", 950_065),
    ("org", 13_169_878),
    ("basic_monthly", 31_922_804),
]


def upload_table(slug: str, expected_rows: int | None) -> int:
    path = PARQUET_ROOT / slug
    if not path.exists():
        raise FileNotFoundError(f"Missing parquet path: {path}")

    tb = bd.Table(dataset_id=DATASET_ID, table_id=slug)

    # Delete stale GCS staging prefix (avoids BQ partition key conflicts)
    st = bd.Storage(dataset_id=DATASET_ID, table_id=slug)
    try:
        st.delete_table(mode="staging", not_found_ok=True)
    except Exception as e:
        print(f"  [warn] staging prefix cleanup: {e}", flush=True)

    tb.create(
        path=str(path),
        source_format="parquet",
        if_table_exists="replace",
        if_storage_data_exists="replace",
        if_dataset_exists="pass",
    )

    client = bigquery.Client(project=BILLING_PROJECT)
    q = f"select count(*) as n from `{BILLING_PROJECT}.{DATASET_ID}_staging.{slug}`"
    n = next(iter(client.query(q).result())).n

    if expected_rows is None:
        print(f"  {slug}: uploaded {n:,} rows", flush=True)
        return n

    status = "OK" if n == expected_rows else "ROW MISMATCH"
    print(
        f"  {slug}: uploaded {n:,} rows (expected {expected_rows:,}) — {status}",
        flush=True,
    )
    if n != expected_rows:
        raise ValueError(
            f"{slug}: row count {n:,} != expected {expected_rows:,}"
        )
    return n


def main():
    only = set(sys.argv[1:])
    known = {s for s, _ in TABLES}
    unknown = only - known
    if unknown:
        print(
            f"unknown table slug(s): {sorted(unknown)}; "
            f"valid: {sorted(known)}",
            flush=True,
        )
        sys.exit(2)
    tables = [(s, r) for s, r in TABLES if not only or s in only]
    for slug, expected in tables:
        print(f"=== {slug} ===", flush=True)
        try:
            upload_table(slug, expected)
        except Exception as e:
            print(f"  FAILED: {type(e).__name__}: {e}", flush=True)
            sys.exit(1)
    print("ALL TABLES UPLOADED", flush=True)


if __name__ == "__main__":
    main()
