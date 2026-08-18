"""Upload the cleaned parquet of us_fec_campaign_finance to BigQuery dev staging.

    python upload.py                 # every table
    python upload.py candidate       # one table

Targets basedosdados-dev only. Prod table data is never uploaded from here: it is
materialized by the GitHub table-approve action when the onboarding PR merges
(.claude/rules/onboarding-workflow.md).

Verifies each staging table's BigQuery row count against the local parquet row count
and stops at the first mismatch, so a partial upload cannot be mistaken for a good one.

Requires ~/.basedosdados/config.toml. The GCS bucket is requester-pays, so
gcs.Client.bucket is monkeypatched to pin user_project to the billing project.
"""

import os
import subprocess
import sys
from pathlib import Path

import basedosdados as bd
import google.cloud.storage as gcs
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from pipelines.datasets.us_fec_campaign_finance import (
    utils as fec,
)

BILLING_PROJECT = "basedosdados-dev"

# FEC_RSYNC=1 uploads via gsutil rsync instead of bd.Table.create's python
# resumable session — required for contribution_individual (15 GB).
RSYNC = os.environ.get("FEC_RSYNC") == "1"
DATASET_ID = "us_fec_campaign_finance"

TABLES = [
    "candidate",
    "committee",
    "candidate_committee_link",
    "contribution_individual",
    "contribution_committee",
    "committee_transaction",
    "disbursement",
    "dicionario",
]

_orig_bucket = gcs.Client.bucket


def _patched_bucket(self, bucket_name, user_project=None):
    return _orig_bucket(self, bucket_name, user_project=BILLING_PROJECT)


gcs.Client.bucket = _patched_bucket


def local_rows(table: str) -> tuple[int, int]:
    files = sorted((fec.OUTPUT / table).rglob("*.parquet"))
    return sum(pq.ParquetFile(f).metadata.num_rows for f in files), len(files)


def bq_rows(table: str) -> int:
    result = bd.read_sql(
        f"select count(*) as n from "
        f"`{BILLING_PROJECT}.{DATASET_ID}_staging.{table}`",
        billing_project_id=BILLING_PROJECT,
        from_file=True,
    )
    return int(result["n"].iloc[0])


def rsync_to_gcs(table: str) -> None:
    """Push a table's parquet tree to its staging prefix with gsutil rsync.

    bd.Table.create uploads through a single-threaded python resumable session, which
    does not survive a 15 GB transfer: contribution_individual died partway with
    `SSLError(5, '[SYS] unknown error')` after ~20 minutes, with no resume. gsutil
    rsync is parallel, retries on its own, and skips blobs already present, so a
    failure costs only the file it was on.

    The blob layout is identical either way — staging/<ds>/<table>/cycle=YYYY/data.parquet
    — so the external table cannot tell which path uploaded it.
    """
    src = str(fec.OUTPUT / table)
    dest = f"gs://{BILLING_PROJECT}/staging/{DATASET_ID}/{table}"
    print(f"[{table}] rsync {src} -> {dest}")
    subprocess.run(
        ["gsutil", "-m", "-u", BILLING_PROJECT, "rsync", "-r", src, dest],
        check=True,
    )


def create_external_table(table: str) -> None:
    """Create the staging external table over an already-populated GCS prefix.

    Schema comes from the 0-row 00_header.parquet, so this never reads a multi-GB
    file into pandas — the same reason that stub exists for the table-approve action.
    """
    bq_table = bd.Table(dataset_id=DATASET_ID, table_id=table)
    bq_table.create(
        path=str(fec.OUTPUT / table),
        source_format="parquet",
        if_table_exists="replace",
        if_storage_data_exists="pass",  # data is already there from the rsync
        if_dataset_exists="pass",
    )


def upload_table(table: str) -> None:
    path = fec.OUTPUT / table
    if not path.exists():
        raise SystemExit(
            f"{table}: nothing cleaned at {path} — run clean.py first"
        )

    expected, nfiles = local_rows(table)
    print(f"[{table}] local: {expected:,} rows in {nfiles} parquet file(s)")

    if RSYNC:
        # Large tables: rsync is resumable and skips what is already uploaded.
        rsync_to_gcs(table)
        create_external_table(table)
    else:
        # Clear the staging prefix first: leftover objects from an earlier shape
        # produce BigQuery partition-key conflicts on the external table.
        storage = bd.Storage(dataset_id=DATASET_ID, table_id=table)
        storage.delete_table(mode="staging", not_found_ok=True)

        bq_table = bd.Table(dataset_id=DATASET_ID, table_id=table)
        bq_table.create(
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
        "\nall requested tables uploaded to "
        f"{BILLING_PROJECT}.{DATASET_ID}_staging"
    )


if __name__ == "__main__":
    main(sys.argv[1:])
