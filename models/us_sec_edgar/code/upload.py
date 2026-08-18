"""Upload the cleaned us_sec_edgar parquet to BigQuery staging (basedosdados-dev).

    uv run python models/us_sec_edgar/code/upload.py                # all tables
    uv run python models/us_sec_edgar/code/upload.py numeric_fact   # one table

The four data tables are hive-partitioned (`<table>/year=<YYYY>/quarter=<Q>/data.parquet`);
`dicionario` is a single file. Each staging table's BigQuery row count is checked against
the local parquet row count and the run stops at the first mismatch.

`2009q1` is header-only at the source, so `year=2009/quarter=1/data.parquet` holds zero
rows. It sorts first, which keeps both `bd.Table.create` and the CI table-approve action
off the "read the first parquet whole" OOM path — do not delete it.

Requires GOOGLE_APPLICATION_CREDENTIALS pointing at a BD dev service-account key plus
`~/.basedosdados/config.toml`. The GCS bucket is requester-pays, so `gcs.Client.bucket`
is monkeypatched to pin `user_project` to the billing project.
"""

import glob
import os
import sys

import basedosdados as bd
import google.cloud.storage as gcs
import pyarrow.parquet as pq

from pipelines.datasets.us_sec_edgar.constants import constants

BILLING_PROJECT = "basedosdados-dev"
DATASET_ID = constants.DATASET_ID.value
TABLES = constants.TABLES.value
OUTPUT = os.path.join(constants.SCRATCH_DIR.value, "output")

_orig_bucket = gcs.Client.bucket


def _patched_bucket(self, bucket_name, user_project=None):
    return _orig_bucket(self, bucket_name, user_project=BILLING_PROJECT)


gcs.Client.bucket = _patched_bucket


def local_rows(table_slug: str):
    files = sorted(
        glob.glob(
            os.path.join(OUTPUT, table_slug, "**", "*.parquet"), recursive=True
        )
    )
    return sum(pq.ParquetFile(f).metadata.num_rows for f in files), len(files)


def upload_table(table_slug: str) -> int:
    path = os.path.join(OUTPUT, table_slug)
    expected, nfiles = local_rows(table_slug)
    if not nfiles:
        raise ValueError(f"no parquet found for {table_slug} under {path}")
    print(
        f"[{table_slug}] local: {expected:,} rows across {nfiles} parquet file(s)"
    )

    storage = bd.Storage(dataset_id=DATASET_ID, table_id=table_slug)
    storage.delete_table(mode="staging", not_found_ok=True)

    table = bd.Table(dataset_id=DATASET_ID, table_id=table_slug)
    table.create(
        path=path,
        source_format="parquet",
        if_table_exists="replace",
        if_storage_data_exists="replace",
        if_dataset_exists="pass",
    )

    query = (
        f"select count(*) as n from "
        f"`{BILLING_PROJECT}.{DATASET_ID}_staging.{table_slug}`"
    )
    got = int(
        bd.read_sql(query, billing_project_id=BILLING_PROJECT, from_file=True)[
            "n"
        ].iloc[0]
    )
    verdict = "MATCH" if got == expected else "MISMATCH"
    print(
        f"[{table_slug}] uploaded - bq={got:,} expected={expected:,} {verdict}"
    )
    if got != expected:
        raise ValueError(f"row count mismatch for {table_slug}")
    return got


if __name__ == "__main__":
    for slug in sys.argv[1:] or TABLES:
        if slug not in TABLES:
            raise SystemExit(f"unknown table {slug!r}")
        upload_table(slug)
    print("done.")
