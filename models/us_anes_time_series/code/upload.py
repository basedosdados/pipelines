"""Upload the ANES Time Series parquet tables to BigQuery dev (basedosdados-dev).

Credentials come from ~/.basedosdados/config.toml via the basedosdados package's
own clients (Base) — GOOGLE_APPLICATION_CREDENTIALS is NOT required.

Tables (uploaded in order; stops on first failure):
    cumulative   hive-partitioned parquet dir (year=YYYY in the path, not files)
    dicionario   single-file parquet dir

Usage:
    .venv/bin/python models/us_anes_time_series/code/upload.py [table_slug ...]
"""

import sys
import warnings
from pathlib import Path

warnings.filterwarnings("ignore")

import basedosdados as bd  # noqa: E402
import google.cloud.storage as gcs  # noqa: E402
from basedosdados.core.base import Base  # noqa: E402

BILLING_PROJECT = "basedosdados-dev"
DATASET_ID = "us_anes_time_series"

OUTPUT_ROOT = Path(
    "/Users/rdahis/Monash Uni Enterprise Dropbox/Ricardo Dahis/BD/pipelines/"
    ".claude/worktrees/anes-dataset-onboarding-d51b97/"
    "models/us_anes_time_series/output"
)

# Monkey-patch for the requester-pays GCS bucket: force user_project.
_orig_bucket = gcs.Client.bucket


def _patched_bucket(self, bucket_name, user_project=None):
    return _orig_bucket(self, bucket_name, user_project=BILLING_PROJECT)


gcs.Client.bucket = _patched_bucket

# (table_slug, expected_rows, source_dir) — cumulative first.
TABLES = [
    ("cumulative", 73_745, OUTPUT_ROOT / "cumulative"),
    ("dicionario", 4_879, OUTPUT_ROOT / "dicionario"),
]

# basedosdados clients built from config.toml (no ADC / env var needed).
_BQ_STAGING = Base().client["bigquery_staging"]


def _staging_count(slug: str) -> int:
    q = (
        f"select count(*) as n "
        f"from `{BILLING_PROJECT}.{DATASET_ID}_staging.{slug}`"
    )
    return next(iter(_BQ_STAGING.query(q).result())).n


def upload_table(slug: str, expected_rows: int, path: Path) -> int:
    if not path.exists():
        raise FileNotFoundError(f"Missing parquet path: {path}")

    # Delete stale GCS staging prefix (avoids BQ partition key conflicts).
    st = bd.Storage(dataset_id=DATASET_ID, table_id=slug)
    try:
        st.delete_table(mode="staging", not_found_ok=True)
    except Exception as e:
        print(f"  [warn] staging prefix cleanup: {e}", flush=True)

    tb = bd.Table(dataset_id=DATASET_ID, table_id=slug)
    tb.create(
        path=str(path),
        source_format="parquet",
        if_table_exists="replace",
        if_storage_data_exists="replace",
        if_dataset_exists="pass",
    )

    n = _staging_count(slug)
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


def main() -> None:
    only = set(sys.argv[1:])
    known = {s for s, _, _ in TABLES}
    unknown = only - known
    if unknown:
        print(
            f"unknown table slug(s): {sorted(unknown)}; valid: {sorted(known)}"
        )
        sys.exit(2)

    tables = [t for t in TABLES if not only or t[0] in only]
    for slug, expected, path in tables:
        print(f"=== {slug} ===", flush=True)
        try:
            upload_table(slug, expected, path)
        except Exception as e:
            print(f"  FAILED: {type(e).__name__}: {e}", flush=True)
            sys.exit(1)
    print("ALL TABLES UPLOADED", flush=True)


if __name__ == "__main__":
    main()
