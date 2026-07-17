"""Upload cleaned parquet tables of us_census_cbp to BigQuery staging (basedosdados-dev).

national/state/county are hive-partitioned by year (year=<YYYY>/data.parquet);
dicionario is a single file. Verifies each staging table's row count against the
local parquet row count. Stops at the first failure.

Requires GOOGLE_APPLICATION_CREDENTIALS pointing at a BD service-account key with
write access to basedosdados-dev, plus ~/.basedosdados/config.toml.
"""

import glob
import sys
from pathlib import Path

import basedosdados as bd
import google.cloud.storage as gcs
import pyarrow.parquet as pq

BILLING_PROJECT = "basedosdados-dev"
DATASET_ID = "us_census_cbp"
ROOT = Path(__file__).resolve().parent.parent
OUTPUT_DIR = ROOT / "data" / "output"
TABLES = ["national", "state", "county", "dicionario"]

_orig_bucket = gcs.Client.bucket


def _patched_bucket(self, bucket_name, user_project=None):
    return _orig_bucket(self, bucket_name, user_project=BILLING_PROJECT)


gcs.Client.bucket = _patched_bucket


def local_rows(table_slug):
    files = glob.glob(
        str(OUTPUT_DIR / table_slug / "**" / "*.parquet"), recursive=True
    )
    return sum(pq.ParquetFile(f).metadata.num_rows for f in files), len(files)


def upload_table(table_slug):
    path = OUTPUT_DIR / table_slug
    expected, nfiles = local_rows(table_slug)
    print(
        f"[{table_slug}] local: {expected:,} rows across {nfiles} parquet file(s)"
    )

    st = bd.Storage(dataset_id=DATASET_ID, table_id=table_slug)
    st.delete_table(mode="staging", not_found_ok=True)

    tb = bd.Table(dataset_id=DATASET_ID, table_id=table_slug)
    tb.create(
        path=str(path),
        source_format="parquet",
        if_table_exists="replace",
        if_storage_data_exists="replace",
        if_dataset_exists="pass",
    )

    query = (
        f"select count(*) as n from "
        f"`{BILLING_PROJECT}.{DATASET_ID}_staging.{table_slug}`"
    )
    df = bd.read_sql(query, billing_project_id=BILLING_PROJECT, from_file=True)
    n = int(df["n"].iloc[0])
    ok = "MATCH" if n == expected else "MISMATCH"
    print(f"[{table_slug}] uploaded - bq={n:,} expected={expected:,} {ok}")
    if n != expected:
        raise ValueError(
            f"Row-count mismatch for {table_slug}: bq={n}, local={expected}"
        )
    return n


def main():
    only = sys.argv[1:] or TABLES
    for table_slug in TABLES:
        if table_slug not in only:
            continue
        try:
            upload_table(table_slug)
        except Exception as e:
            print(f"[{table_slug}] FAILED - {e}")
            sys.exit(1)
    print("All CBP uploads complete.")


if __name__ == "__main__":
    main()
