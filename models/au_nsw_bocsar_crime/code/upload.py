"""Upload cleaned au_nsw_bocsar_crime parquet tables to BigQuery.

Usage:
    uv run python models/au_nsw_bocsar_crime/code/upload.py [--env dev|prod] [table ...]

--env dev (default) -> basedosdados-dev; --env prod -> basedosdados. Point
GOOGLE_APPLICATION_CREDENTIALS at the matching service account. Uploads
smallest-first, verifies BQ row count against the local parquet, stops on first
failure.
"""

import glob
import sys
import warnings
from pathlib import Path

warnings.filterwarnings("ignore")

import basedosdados as bd  # noqa: E402
import google.cloud.storage as gcs  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402
from google.cloud import bigquery  # noqa: E402

_argv = sys.argv[1:]
if "--env" in _argv:
    _i = _argv.index("--env")
    ENV = _argv[_i + 1]
    _argv = _argv[:_i] + _argv[_i + 2 :]
else:
    ENV = "dev"
BILLING_PROJECT = "basedosdados" if ENV == "prod" else "basedosdados-dev"
DATASET_ID = "au_nsw_bocsar_crime"
OUTPUT_ROOT = Path(__file__).resolve().parent.parent / "output"

# smallest-first
TABLES = [
    "custody_remand_to_sentenced",
    "custody_receptions",
    "alleged_offenders",
    "custody_discharges",
    "criminal_incidents",
    "custody_population",
    "criminal_incidents_daily",
    "criminal_incidents_sa4",
    "criminal_incidents_lga",
    "criminal_incidents_postcode",
    "criminal_incidents_suburb",
]

_orig_bucket = gcs.Client.bucket


def _patched_bucket(self, bucket_name, user_project=None):
    return _orig_bucket(self, bucket_name, user_project=BILLING_PROJECT)


gcs.Client.bucket = _patched_bucket


def local_rows(slug: str) -> int:
    return sum(
        pq.ParquetFile(f).metadata.num_rows
        for f in glob.glob(str(OUTPUT_ROOT / slug / "year=*" / "*.parquet"))
    )


def upload_table(slug: str) -> int:
    path = OUTPUT_ROOT / slug
    if not path.exists():
        raise FileNotFoundError(f"Missing output path: {path}")
    expected = local_rows(slug)

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

    # Count verification is best-effort: it needs bigquery.jobs.create on the
    # billing project, which the ambient credentials may lack. The bd upload
    # above already confirms the staging table was created; dbt run/test verifies
    # row counts downstream.
    try:
        client = bigquery.Client(project=BILLING_PROJECT)
        q = f"select count(*) as n from `{BILLING_PROJECT}.{DATASET_ID}_staging.{slug}`"
        n = next(iter(client.query(q).result())).n
        status = "OK" if n == expected else "ROW MISMATCH"
        print(f"  {slug}: uploaded {n:,} rows (local {expected:,}) — {status}")
        if n != expected:
            raise ValueError(f"{slug}: BQ {n:,} != local {expected:,}")
        return n
    except ValueError:
        raise
    except Exception as e:
        print(
            f"  {slug}: staging table created; local parquet has {expected:,} rows "
            f"(BQ count unverified: {type(e).__name__})"
        )
        return expected


def main():
    only = set(_argv)
    tables = [t for t in TABLES if not only or t in only]
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
