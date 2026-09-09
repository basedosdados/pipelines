"""Upload cleaned world_noaa_ghcn parquet to BigQuery staging (basedosdados-dev).

    python models/world_noaa_ghcn/code/upload.py                 # all tables
    python models/world_noaa_ghcn/code/upload.py station         # one table

`observation` is hive-partitioned by year (`year=<YYYY>/data.parquet`); the
other three are a single file each. Each staging table's BigQuery row count is
verified against the local parquet count and the first mismatch stops the run.

`bd.Table.create` is used rather than a direct BigQuery load job because it
creates an **external** table over the GCS prefix. A load job would create a
NATIVE table, which the recurring pipeline would then silently shadow: the flow
writes only to GCS, so dbt would keep serving the stale native snapshot.

Requires GOOGLE_APPLICATION_CREDENTIALS pointing at a BD dev service-account key
plus ~/.basedosdados/config.toml. The GCS bucket is requester-pays, so
gcs.Client.bucket is monkeypatched to pin user_project to the billing project.
"""

from __future__ import annotations

import glob
import os
import sys
from pathlib import Path

import basedosdados as bd
import google.cloud.storage as gcs
import pyarrow.parquet as pq

DATASET_ID = "world_noaa_ghcn"
BILLING_PROJECT = "basedosdados-dev"
OUTPUT = Path(
    os.environ.get("GHCN_DATA_DIR", os.path.expanduser("~/Downloads/world_noaa_ghcn_data"))
) / "output"

# Smallest first, so a credentials or permissions problem surfaces in seconds
# rather than after the multi-gigabyte observation upload.
TABLES = ["dicionario", "station", "station_element_inventory", "observation"]

_orig_bucket = gcs.Client.bucket


def _patched_bucket(self, bucket_name, user_project=None):
    return _orig_bucket(self, bucket_name, user_project=BILLING_PROJECT)


gcs.Client.bucket = _patched_bucket


def local_rows(table: str) -> tuple[int, int]:
    files = glob.glob(str(OUTPUT / table / "**" / "*.parquet"), recursive=True)
    return sum(pq.ParquetFile(f).metadata.num_rows for f in files), len(files)


def upload_table(table: str) -> int:
    path = OUTPUT / table
    expected, nfiles = local_rows(table)
    if nfiles == 0:
        raise ValueError(f"no parquet found under {path}; run backfill.py first")
    print(f"[{table}] local: {expected:,} rows across {nfiles} parquet file(s)", flush=True)

    st = bd.Storage(dataset_id=DATASET_ID, table_id=table)
    st.delete_table(mode="staging", not_found_ok=True)

    tb = bd.Table(dataset_id=DATASET_ID, table_id=table)
    tb.create(
        path=str(path),
        source_format="parquet",
        if_table_exists="replace",
        if_storage_data_exists="replace",
        if_dataset_exists="pass",
    )

    query = (
        f"select count(*) as n from "
        f"`{BILLING_PROJECT}.{DATASET_ID}_staging.{table}`"
    )
    df = bd.read_sql(query, billing_project_id=BILLING_PROJECT, from_file=True)
    n = int(df["n"].iloc[0])
    ok = "MATCH" if n == expected else "MISMATCH"
    print(f"[{table}] uploaded - bq={n:,} expected={expected:,} {ok}", flush=True)
    if n != expected:
        raise ValueError(f"row count mismatch for {table}")
    return n


if __name__ == "__main__":
    tables = sys.argv[1:] or TABLES
    for t in tables:
        if t not in TABLES:
            raise SystemExit(f"unknown table {t!r}")
        upload_table(t)
    print("done.")
