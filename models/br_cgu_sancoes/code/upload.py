"""Upload cleaned typed Parquet for br_cgu_sancoes to BigQuery dev staging.

Usage:
    uv run python models/br_cgu_sancoes/code/upload.py [--table <slug>]

Dev only: billing/target project is basedosdados-dev. Point
GOOGLE_APPLICATION_CREDENTIALS at the dev service account. Reads the cleaned
hive-partitioned parquet from $BR_CGU_SANCOES_DATA/output (default
~/Downloads/br_cgu_sancoes_data/output), which is NEVER under the repo or Dropbox.
This is the one-shot onboarding upload path: keep the TYPED parquet schema (do not
cast to all-STRING). Uploads smallest table first and stops on first failure.
"""

import argparse
import os
import warnings
from pathlib import Path

warnings.filterwarnings("ignore")

import basedosdados as bd  # noqa: E402
import google.cloud.storage as gcs  # noqa: E402
from google.cloud import bigquery  # noqa: E402

BILLING_PROJECT = "basedosdados-dev"  # DEV ONLY — never prod
DATASET_ID = "br_cgu_sancoes"
DATA_ROOT = Path(
    os.environ.get(
        "BR_CGU_SANCOES_DATA",
        Path.home() / "Downloads" / "br_cgu_sancoes_data",
    )
)
OUTPUT_ROOT = DATA_ROOT / "output"

# Monkey-patch for requester-pays bucket: force user_project = billing project.
_orig_bucket = gcs.Client.bucket


def _patched_bucket(self, bucket_name, user_project=None, **kwargs):
    return _orig_bucket(
        self, bucket_name, user_project=BILLING_PROJECT, **kwargs
    )


gcs.Client.bucket = _patched_bucket

# (table_slug, expected_rows) — smallest first
TABLES = [
    ("dicionario", 4),
    ("acordos_leniencia", 151),
    ("acordos_leniencia_efeitos", 173),
    ("cnep", 1_757),
    ("cepim", 3_537),
    ("ceis", 23_547),
]


def upload_table(slug: str, expected_rows: int) -> int:
    path = OUTPUT_ROOT / slug
    if not path.exists():
        raise FileNotFoundError(f"Missing output path: {path}")

    tb = bd.Table(dataset_id=DATASET_ID, table_id=slug)

    # Delete stale GCS staging prefix before upload (avoids BQ partition conflicts).
    st = bd.Storage(dataset_id=DATASET_ID, table_id=slug)
    try:
        st.delete_table(mode="staging", not_found_ok=True)
    except Exception as e:
        print(f"  [warn] staging prefix cleanup: {e}")

    tb.create(
        path=str(path),
        source_format="parquet",
        if_table_exists="replace",
        if_storage_data_exists="replace",
        if_dataset_exists="pass",
    )

    client = bigquery.Client(project=BILLING_PROJECT)
    fqn = f"{BILLING_PROJECT}.{DATASET_ID}_staging.{slug}"
    n = next(
        iter(client.query(f"select count(*) as n from `{fqn}`").result())
    ).n
    status = "OK" if n == expected_rows else "MISMATCH"
    print(f"  {slug}: {n:,} rows (expected {expected_rows:,}) — {status}")
    print(f"  staging: {fqn}")
    if n != expected_rows:
        raise SystemExit(f"  {slug}: row count mismatch — aborting")
    return n


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", default=None, help="Upload only this table")
    args = parser.parse_args()

    known = {t[0] for t in TABLES}
    if args.table is not None and args.table not in known:
        raise SystemExit(
            f"Unknown --table {args.table!r}; choose from {sorted(known)}"
        )
    targets = [args.table] if args.table else [t[0] for t in TABLES]
    print(f"env=dev billing={BILLING_PROJECT} dataset={DATASET_ID}")
    results = {}
    for slug, expected in TABLES:
        if slug in targets:
            print(f"[upload] {slug}")
            results[slug] = upload_table(slug, expected)

    print("\n=== UPLOAD COMPLETE: br_cgu_sancoes (env=dev) ===")
    for slug in results:
        print(f"  OK {slug}: {results[slug]:,} rows")


if __name__ == "__main__":
    main()
