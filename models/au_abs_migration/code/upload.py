"""Upload the cleaned tables to BigQuery dev staging.

Writes gs://basedosdados-dev/staging/au_abs_migration/<table> and creates the
external table basedosdados-dev.au_abs_migration_staging.<table>, then verifies
the row count in BigQuery against the parquet actually on disk rather than a
hardcoded number, so a re-clean that changes the data cannot pass a stale
expectation.

Run:  python upload.py                                    # every table
      python upload.py overseas_country_of_birth_state    # a subset
"""

from __future__ import annotations

import os
import pathlib
import sys

import basedosdados as bd
import google.cloud.storage as gcs
import pyarrow.parquet as pq

BILLING_PROJECT = "basedosdados-dev"
DATASET_ID = "au_abs_migration"

DATA_DIR = pathlib.Path(
    os.environ.get(
        "AU_ABS_MIGRATION_DATA",
        pathlib.Path.home() / "Downloads" / "au_abs_migration_data",
    )
)
OUTPUT_DIR = DATA_DIR / "output"

# The staging bucket is requester-pays, so every bucket handle needs a billing
# project attached.
_original_bucket = gcs.Client.bucket


def _bucket_with_billing(self, bucket_name, user_project=None):
    return _original_bucket(self, bucket_name, user_project=BILLING_PROJECT)


gcs.Client.bucket = _bucket_with_billing

TABLES = [
    "overseas_country_of_birth_australia",
    "overseas_country_of_birth_state",
    "overseas_age_sex_australia",
    "overseas_age_sex_state",
    "overseas_age_sex_australia_calendar_year",
    "overseas_age_sex_state_calendar_year",
    "overseas_visa_australia",
    "overseas_visa_state",
    "overseas_visa_quarter_australia",
    "overseas_visa_quarter_state",
    "interstate_age_sex_australia",
    "interstate_age_sex_state",
    "interstate_age_sex_australia_calendar_year",
    "interstate_age_sex_state_calendar_year",
    "dicionario",
]


def local_rows(root: pathlib.Path) -> int:
    return sum(
        pq.read_metadata(path).num_rows
        for path in sorted(root.rglob("*.parquet"))
    )


def upload_table(table: str, output_dir: pathlib.Path = OUTPUT_DIR) -> None:
    path = output_dir / table
    if not path.exists():
        raise FileNotFoundError(f"no cleaned output at {path}")
    expected = local_rows(path)

    # Clear the staging prefix first: a partition left from an earlier run would
    # otherwise survive and be read alongside the current data.
    storage = bd.Storage(dataset_id=DATASET_ID, table_id=table)
    storage.delete_table(mode="staging", not_found_ok=True)

    bd.Table(dataset_id=DATASET_ID, table_id=table).create(
        path=str(path),
        source_format="parquet",
        if_table_exists="replace",
        if_storage_data_exists="replace",
        if_dataset_exists="pass",
    )

    query = f"select count(*) as n from `{BILLING_PROJECT}.{DATASET_ID}_staging.{table}`"
    got = int(
        bd.read_sql(query, billing_project_id=BILLING_PROJECT, from_file=True)[
            "n"
        ].iloc[0]
    )
    verdict = "MATCH" if got == expected else "MISMATCH"
    print(f"[{table}] bq={got:,} local={expected:,} {verdict}")
    if got != expected:
        raise ValueError(
            f"{table}: BigQuery has {got} rows, parquet has {expected}"
        )


def main() -> None:
    wanted = sys.argv[1:] or TABLES
    unknown = [table for table in wanted if table not in TABLES]
    if unknown:
        raise SystemExit(f"unknown table(s): {unknown}")

    for table in wanted:
        try:
            upload_table(table)
        except Exception as error:  # stop at the first failure, per house rule
            print(f"[{table}] FAILED — {error}")
            sys.exit(1)
    print("\nAll uploads complete.")


if __name__ == "__main__":
    main()
