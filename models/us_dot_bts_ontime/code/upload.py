"""Upload the cleaned parquet of us_dot_bts_ontime to BigQuery dev staging.

    uv run --no-project --with basedosdados --with pyarrow --with pandas \
        python models/us_dot_bts_ontime/code/upload.py            # every table
    ... python models/us_dot_bts_ontime/code/upload.py airport    # one table

Targets basedosdados-dev only. Prod table data is never uploaded from here: it is
materialized by the GitHub table-approve action when the onboarding PR merges
(.claude/rules/onboarding-workflow.md).

Each staging table's BigQuery row count is verified against the local parquet row
count, and the script stops at the first mismatch so a partial upload cannot be
mistaken for a good one.

Requires ~/.basedosdados/config.toml. The GCS bucket is requester-pays, so
gcs.Client.bucket is monkeypatched to pin user_project to the billing project.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import basedosdados as bd
import google.cloud.storage as gcs
import pyarrow.parquet as pq

BILLING_PROJECT = "basedosdados-dev"
DATASET_ID = "us_dot_bts_ontime"
OUTPUT = (
    Path(
        os.environ.get(
            "BTS_DATA_DIR",
            Path.home() / "Downloads" / "us_dot_bts_ontime_data",
        )
    )
    / "output"
)

TABLES = ["flight", "airport", "dicionario"]

_orig_bucket = gcs.Client.bucket


def _patched_bucket(self, bucket_name, user_project=None):
    return _orig_bucket(self, bucket_name, user_project=BILLING_PROJECT)


gcs.Client.bucket = _patched_bucket


def local_rows(table: str) -> tuple[int, int]:
    files = sorted((OUTPUT / table).rglob("*.parquet"))
    return sum(pq.ParquetFile(f).metadata.num_rows for f in files), len(files)


def bq_rows(table: str) -> int:
    result = bd.read_sql(
        f"select count(*) as n from `{BILLING_PROJECT}.{DATASET_ID}_staging.{table}`",
        billing_project_id=BILLING_PROJECT,
        from_file=True,
    )
    return int(result["n"].iloc[0])


def upload_table(table: str) -> None:
    path = OUTPUT / table
    if not path.exists():
        raise SystemExit(
            f"{table}: nothing cleaned at {path} — run clean.py first"
        )

    expected, nfiles = local_rows(table)
    print(f"[{table}] local: {expected:,} rows in {nfiles} parquet file(s)")

    # Clear the staging prefix first: leftover objects from an earlier shape
    # produce BigQuery partition-key conflicts on the external table.
    #
    # No rsync fast path here. `flight` is ~11 GB, but it is 465 files of ~25 MB
    # rather than one huge blob, so a failed transfer costs a single file and the
    # single-threaded uploader copes. An rsync branch was tried and removed: it
    # bought nothing, because bd.Table.create re-uploads the tree regardless of
    # if_storage_data_exists="pass", and it carried a real hazard — rsync adds and
    # overwrites but never deletes, so a resumed run could leave a stale partition
    # layout that the external table reads alongside the new one.
    bd.Storage(dataset_id=DATASET_ID, table_id=table).delete_table(
        mode="staging", not_found_ok=True
    )
    bd.Table(dataset_id=DATASET_ID, table_id=table).create(
        path=str(path),
        source_format="parquet",
        if_table_exists="replace",
        if_storage_data_exists="replace",
        if_dataset_exists="pass",
    )

    got = bq_rows(table)
    if got != expected:
        raise SystemExit(
            f"{table}: BigQuery has {got:,} rows, local parquet has {expected:,}"
        )
    print(f"[{table}] uploaded and verified: {got:,} rows")


def main(argv: list[str]) -> None:
    tables = argv or TABLES
    unknown = set(tables) - set(TABLES)
    if unknown:
        raise SystemExit(f"unknown table(s): {sorted(unknown)}")
    for table in tables:
        upload_table(table)
    print(
        f"\nall requested tables uploaded to {BILLING_PROJECT}.{DATASET_ID}_staging"
    )


if __name__ == "__main__":
    main(sys.argv[1:])
