"""Upload the cleaned tables to BigQuery dev staging.

Writes ``gs://basedosdados-dev/staging/au_treasury_budget/<table>`` and creates
the external table ``basedosdados-dev.au_treasury_budget_staging.<table>``, then
verifies each row count in BigQuery against the Parquet actually on disk rather
than against a number written here, so a re-clean that changes the data cannot
pass a stale expectation.

Run:  python upload.py                 # every table
      python upload.py aggregate       # a subset
"""

from __future__ import annotations

import os
import pathlib
import sys

import basedosdados as bd
import google.cloud.storage as gcs
import pyarrow.parquet as pq

BILLING_PROJECT = "basedosdados-dev"
DATASET_ID = "au_treasury_budget"

DATA_DIR = pathlib.Path(
    os.environ.get(
        "AU_TREASURY_BUDGET_DATA",
        pathlib.Path.home() / "Downloads" / "au_treasury_budget_data",
    )
)
OUTPUT_DIR = DATA_DIR / "output"

TABLES = ("aggregate", "payment_growth", "igr_projection", "dicionario")

# The staging bucket is requester-pays, so every bucket handle needs a billing
# project attached or the upload fails with UserProjectMissing.
_original_bucket = gcs.Client.bucket


def _bucket_with_billing(self, bucket_name, user_project=None):
    return _original_bucket(self, bucket_name, user_project=BILLING_PROJECT)


gcs.Client.bucket = _bucket_with_billing


def local_rows(root: pathlib.Path) -> int:
    return sum(
        pq.read_metadata(path).num_rows
        for path in sorted(root.rglob("*.parquet"))
    )


def upload_table(table: str) -> int:
    path = OUTPUT_DIR / table
    if not path.exists():
        raise FileNotFoundError(
            f"no cleaned output at {path}. Run clean.py first."
        )
    expected = local_rows(path)

    # Clear the staging prefix first. A partition left behind by an earlier run
    # survives an overwrite and is read alongside the current data, which shows
    # up as a row count that is too high and nothing else.
    storage = bd.Storage(dataset_id=DATASET_ID, table_id=table)
    storage.delete_table(mode="staging", not_found_ok=True)

    bd.Table(dataset_id=DATASET_ID, table_id=table).create(
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
    got = int(
        bd.read_sql(query, billing_project_id=BILLING_PROJECT, from_file=True)[
            "n"
        ].iloc[0]
    )
    status = "OK" if got == expected else "MISMATCH"
    print(f"  {table:16s} local={expected:7,d} bigquery={got:7,d}  {status}")
    if got != expected:
        raise SystemExit(
            f"{table}: BigQuery has {got:,} rows but the parquet on disk has "
            f"{expected:,}. Stopping rather than continuing to the next table."
        )
    return got


def main() -> int:
    wanted = sys.argv[1:] or list(TABLES)
    unknown = set(wanted) - set(TABLES)
    if unknown:
        raise SystemExit(f"unknown table(s): {sorted(unknown)}")
    total = 0
    for table in wanted:
        total += upload_table(table)
    print(f"\nuploaded {len(wanted)} table(s), {total:,} rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
