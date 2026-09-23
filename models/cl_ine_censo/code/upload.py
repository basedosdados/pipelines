#!/usr/bin/env python3
"""Upload the cleaned cl_ine_censo parquet to BigQuery staging.

Usage::

    python upload.py                      # all tables, dev
    python upload.py persona hogar        # a subset
    python upload.py --env prod           # only after explicit approval

Run it through ``run_guarded.sh`` for the large tables.
"""

from __future__ import annotations

import sys
import warnings

warnings.filterwarnings("ignore")

import basedosdados as bd  # noqa: E402
import google.cloud.storage as gcs  # noqa: E402
import pyarrow.dataset as pads  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402
from basedosdados.upload.datatypes import Datatype  # noqa: E402
from constants import DATASET_ID, OUTPUT_DIR, TABLES  # noqa: E402

_argv = sys.argv[1:]
ENV = "dev"
if "--env" in _argv:
    index = _argv.index("--env")
    ENV = _argv[index + 1]
    _argv = _argv[:index] + _argv[index + 2 :]
if ENV not in ("dev", "prod"):
    sys.exit(f"--env must be 'dev' or 'prod', got {ENV!r}")

BILLING = "basedosdados" if ENV == "prod" else "basedosdados-dev"

# --- Patch 1: requester-pays -----------------------------------------------
# The staging buckets are requester-pays, so every bucket handle needs a billing
# project or the upload 400s.
_original_bucket = gcs.Client.bucket


def _patched_bucket(self, bucket_name, user_project=None):
    return _original_bucket(self, bucket_name, user_project=BILLING)


gcs.Client.bucket = _patched_bucket


# --- Patch 2: do not read a whole parquet file to get its column names -------
# basedosdados 2.0.3 `Datatype.header` does `pd.read_parquet(path)` and then
# returns `list(dataframe.columns.values)` - it loads every row of the sample
# file into pandas purely to read the header. Table.create picks that sample by
# globbing and taking [0], so on this dataset it can land on the 159 MB Santiago
# partition of `persona`, which in pandas is several GB. That is the same class
# of bug that crashed a 16 GB machine during cleaning.
#
# The parquet footer already carries the column names, so this reads metadata
# only and returns exactly the same list.
_original_header = Datatype.header


def _patched_header(self, data_sample_path, csv_delimiter: str = ","):
    if self.source_format == "parquet":
        return list(pq.ParquetFile(str(data_sample_path)).schema_arrow.names)
    return _original_header(self, data_sample_path, csv_delimiter)


Datatype.header = _patched_header


def local_rows(table: str) -> int:
    return pads.dataset(
        OUTPUT_DIR / table, format="parquet", partitioning="hive"
    ).count_rows()


def remote_rows(table: str) -> int:
    """Row count of the staging external table, read without a full scan."""
    client = bd.Base().client["bigquery_staging"]
    query = (
        f"select count(*) as n from `{BILLING}.{DATASET_ID}_staging.{table}`"
    )
    return next(iter(client.query(query).result()))["n"]


def clear_staging_prefix(table: str) -> None:
    """Delete the old staging blobs before re-uploading.

    Without this, a partition that no longer exists locally survives in GCS and
    the external table keeps serving it, which shows up later as phantom rows or
    a BigQuery partition-key conflict.
    """
    storage = bd.Storage(dataset_id=DATASET_ID, table_id=table)
    bucket = storage.client["storage_staging"].bucket(storage.bucket_name)
    prefix = f"staging/{DATASET_ID}/{table}/"
    blobs = list(bucket.list_blobs(prefix=prefix))
    for blob in blobs:
        blob.delete()
    print(f"    cleared {len(blobs)} existing blob(s) at {prefix}")


def upload(table: str) -> None:
    path = OUTPUT_DIR / table
    if not path.exists():
        sys.exit(f"missing cleaned output for {table}: {path}")

    expected = local_rows(table)
    print(f"[{table}] {expected:,} rows -> {BILLING}.{DATASET_ID}_staging")

    clear_staging_prefix(table)

    bq_table = bd.Table(dataset_id=DATASET_ID, table_id=table)
    bq_table.create(
        path=str(path),
        if_table_exists="replace",
        if_storage_data_exists="replace",
        source_format="parquet",
    )

    actual = remote_rows(table)
    status = "OK" if actual == expected else f"MISMATCH (local {expected:,})"
    print(f"    staging rows: {actual:,} {status}")
    if actual != expected:
        sys.exit(
            f"row-count mismatch on {table}; stopping before the next table"
        )


def main() -> None:
    targets = _argv or list(TABLES)
    unknown = [t for t in targets if t not in TABLES]
    if unknown:
        sys.exit(f"unknown table(s): {unknown}")

    for table in targets:
        upload(table)
    print("\nall requested tables uploaded")


if __name__ == "__main__":
    main()
