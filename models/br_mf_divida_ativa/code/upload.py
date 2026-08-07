"""Upload cleaned br_mf_divida_ativa parquet tables to BigQuery.

Reads partitioned parquet from the data root (outside Dropbox; override with
PGFN_DATA_ROOT) and loads each into the dataset's staging. Row count is verified
against the local parquet footers (metadata-only, so cheap even for ~1B rows).

Usage:
    uv run python models/br_mf_divida_ativa/code/upload.py [--env dev|prod] [table ...]

--env dev (default) -> basedosdados-dev; --env prod -> basedosdados. Point
GOOGLE_APPLICATION_CREDENTIALS at the matching service account. Smallest table
first; stops on first failure.
"""

import os
import sys
import warnings
from pathlib import Path

warnings.filterwarnings("ignore")

import basedosdados as bd  # noqa: E402
import google.cloud.storage as gcs  # noqa: E402
import pyarrow.dataset as pads  # noqa: E402
from google.cloud import bigquery  # noqa: E402

_argv = sys.argv[1:]
if "--env" in _argv:
    _i = _argv.index("--env")
    ENV = _argv[_i + 1]
    _argv = _argv[:_i] + _argv[_i + 2 :]
else:
    ENV = "dev"
BILLING_PROJECT = "basedosdados" if ENV == "prod" else "basedosdados-dev"
DATASET_ID = "br_mf_divida_ativa"
DATA_ROOT = Path(
    os.environ.get(
        "PGFN_DATA_ROOT",
        str(Path.home() / "Downloads" / "pgfn_divida_ativa_data"),
    )
)
OUTPUT_ROOT = DATA_ROOT / "output"

# Monkey-patch for requester-pays bucket
_orig_bucket = gcs.Client.bucket


def _patched_bucket(self, bucket_name, user_project=None):
    return _orig_bucket(self, bucket_name, user_project=BILLING_PROJECT)


gcs.Client.bucket = _patched_bucket

# smallest first
TABLES = ["fgts", "previdenciario", "nao_previdenciario"]


def upload_table(slug: str) -> int:
    path = OUTPUT_ROOT / slug
    if not path.exists():
        raise FileNotFoundError(f"Missing output path: {path}")

    expected = pads.dataset(path, format="parquet").count_rows()

    tb = bd.Table(dataset_id=DATASET_ID, table_id=slug)
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

    # Row-count cross-check via a BQ query. Best-effort: the staging external
    # table is created over the exact parquet files, so the local count is
    # authoritative. On dev the QueryUsagePerDay quota can block this query -
    # warn and continue rather than fail the whole upload.
    try:
        client = bigquery.Client(project=BILLING_PROJECT)
        q = f"select count(*) as n from `{BILLING_PROJECT}.{DATASET_ID}_staging.{slug}`"
        n = next(iter(client.query(q).result())).n
        status = "OK" if n == expected else "ROW MISMATCH"
        print(
            f"  {slug}: uploaded {n:,} rows (local parquet {expected:,}) - {status}"
        )
        if n != expected:
            raise ValueError(f"{slug}: BQ {n:,} != local {expected:,}")
    except ValueError:
        raise
    except Exception as e:
        print(
            f"  {slug}: uploaded ~{expected:,} rows (local parquet); "
            f"BQ verify skipped - {type(e).__name__}: {str(e)[:120]}"
        )
    return expected


def main():
    only = set(_argv)
    tables = [s for s in TABLES if not only or s in only]
    print(f"=== uploading to {BILLING_PROJECT} (env={ENV}) ===", flush=True)
    for slug in tables:
        print(f"=== {slug} ===", flush=True)
        try:
            upload_table(slug)
        except Exception as e:
            print(f"  FAILED: {type(e).__name__}: {e}")
            sys.exit(1)
    print("ALL TABLES UPLOADED")


if __name__ == "__main__":
    main()
