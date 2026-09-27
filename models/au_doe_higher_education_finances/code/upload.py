"""Upload the cleaned tables to BigQuery dev staging.

Writes gs://basedosdados-dev/staging/<dataset>/<table> and creates the external
table basedosdados-dev.<dataset>_staging.<table>, then verifies the row count in
BigQuery against the parquet actually on disk rather than a hardcoded number, so
a re-clean that changes the data cannot pass a stale expectation.

The provider directory table belongs to br_bd_diretorios_au and is uploaded into
that dataset's staging, not this one.

Run:  python upload.py                     # every table
      python upload.py research_income     # a subset
"""

from __future__ import annotations

import os
import pathlib
import sys

import basedosdados as bd
import google.cloud.storage as gcs
import pyarrow.parquet as pq

BILLING_PROJECT = "basedosdados-dev"
DATASET_ID = "au_doe_higher_education_finances"
DIRECTORY_DATASET_ID = "br_bd_diretorios_au"

DATA_DIR = pathlib.Path(
    os.environ.get(
        "AU_DOE_HEF_DATA",
        pathlib.Path.home()
        / "Downloads/au_doe_higher_education_finances_data",
    )
)
OUTPUT_DIR = DATA_DIR / "output"
DIRECTORY_OUTPUT_DIR = DATA_DIR / "directory_output"

# The staging bucket is requester-pays, so every bucket handle needs a billing
# project attached.
_original_bucket = gcs.Client.bucket


def _bucket_with_billing(self, bucket_name, user_project=None):
    return _original_bucket(self, bucket_name, user_project=BILLING_PROJECT)


gcs.Client.bucket = _bucket_with_billing

# table -> (dataset it belongs to, its output root)
TABLES = {
    "income_statement": (DATASET_ID, OUTPUT_DIR),
    "balance_sheet": (DATASET_ID, OUTPUT_DIR),
    "equity_movement": (DATASET_ID, OUTPUT_DIR),
    "cash_flow": (DATASET_ID, OUTPUT_DIR),
    "research_income": (DATASET_ID, OUTPUT_DIR),
    "line_item": (DATASET_ID, OUTPUT_DIR),
    "dicionario": (DATASET_ID, OUTPUT_DIR),
    "higher_education_provider": (DIRECTORY_DATASET_ID, DIRECTORY_OUTPUT_DIR),
}


def local_rows(root: pathlib.Path) -> int:
    return sum(
        pq.read_metadata(p).num_rows for p in sorted(root.rglob("*.parquet"))
    )


def upload_table(table: str, dataset_id: str, root: pathlib.Path) -> None:
    path = root / table
    if not path.exists():
        raise FileNotFoundError(f"no cleaned output at {path}")
    expected = local_rows(path)

    # Clear the staging prefix first: a partition left from an earlier run would
    # otherwise survive and be read alongside the current data.
    storage = bd.Storage(dataset_id=dataset_id, table_id=table)
    storage.delete_table(mode="staging", not_found_ok=True)

    bd.Table(dataset_id=dataset_id, table_id=table).create(
        path=str(path),
        source_format="parquet",
        if_table_exists="replace",
        if_storage_data_exists="replace",
        if_dataset_exists="pass",
    )

    query = f"select count(*) as n from `{BILLING_PROJECT}.{dataset_id}_staging.{table}`"
    got = int(
        bd.read_sql(query, billing_project_id=BILLING_PROJECT, from_file=True)[
            "n"
        ].iloc[0]
    )
    verdict = "MATCH" if got == expected else "MISMATCH"
    print(
        f"[{table}] {dataset_id}_staging — bq={got:,} local={expected:,} {verdict}"
    )
    if got != expected:
        raise ValueError(
            f"{table}: BigQuery has {got} rows, parquet has {expected}"
        )


def main() -> None:
    wanted = sys.argv[1:] or list(TABLES)
    unknown = [t for t in wanted if t not in TABLES]
    if unknown:
        raise SystemExit(f"unknown table(s): {unknown}")

    for table in wanted:
        dataset_id, root = TABLES[table]
        try:
            upload_table(table, dataset_id, root)
        except Exception as error:  # stop at the first failure, per house rule
            print(f"[{table}] FAILED — {error}")
            sys.exit(1)
    print("\nAll uploads complete.")


if __name__ == "__main__":
    main()
