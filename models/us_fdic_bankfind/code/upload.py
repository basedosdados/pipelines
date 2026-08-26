"""Upload the cleaned parquet of us_fdic_bankfind to BigQuery dev staging.

    uv run python models/us_fdic_bankfind/code/upload.py            # every table
    uv run python models/us_fdic_bankfind/code/upload.py financials # one table

Targets basedosdados-dev only.  Prod table data is never uploaded from here: it
is materialized by the GitHub table-approve action when the onboarding PR merges
(.claude/rules/onboarding-workflow.md).

Two things this has to work around, both learned on earlier large datasets:

* `bd.Table.create` infers the schema by reading the FIRST file in the prefix
  into pandas.  `financials_indicator` is ~18 GB, so a real partition would blow
  up memory here and again in the table-approve action.  A 0-row
  `00_header.parquet` is written at the table root, which sorts ahead of every
  `year=...` partition and costs nothing to read.
* `bd.Table.create`'s upload is a single-threaded python resumable session that
  does not survive multi-GB transfers.  Data goes up with `gcloud storage rsync`
  instead, and the external table is then created over the already-populated
  prefix with `if_storage_data_exists="pass"`.

Each table's BigQuery row count is verified against the local parquet row count,
stopping at the first mismatch so a partial upload cannot pass for a good one.
"""

from __future__ import annotations

import csv
import os
import subprocess
import sys
from pathlib import Path

import basedosdados as bd
import google.cloud.storage as gcs
import pyarrow as pa
import pyarrow.parquet as pq

BILLING_PROJECT = "basedosdados-dev"
DATASET_ID = "us_fdic_bankfind"
HERE = Path(__file__).resolve().parent
ARCH = HERE / "architecture"
OUT = (
    Path(
        os.environ.get(
            "FDIC_DATA_DIR", Path.home() / "Downloads/us_fdic_bankfind_data"
        )
    )
    / "output"
)

TABLES = ["institution", "indicator", "financials", "financials_indicator"]
UNPARTITIONED = {"institution", "indicator"}
HEADER = "00_header.parquet"

_orig_bucket = gcs.Client.bucket


def _patched_bucket(self, bucket_name, user_project=None):
    # the staging bucket is requester-pays; pin the billing project
    return _orig_bucket(self, bucket_name, user_project=BILLING_PROJECT)


gcs.Client.bucket = _patched_bucket


def columns(table: str) -> list[str]:
    with (ARCH / f"{table}.csv").open() as handle:
        return [row["name"] for row in csv.DictReader(handle)]


def write_header_stub(table: str) -> None:
    """Write the 0-row schema stub the external table is inferred from.

    Built from the architecture rather than copied from a partition: staging is
    all-STRING by convention, so the stub must not inherit a partition's types.
    """
    if table in UNPARTITIONED:
        return
    schema = pa.schema(
        [pa.field(name, pa.string()) for name in columns(table)]
    )
    pq.write_table(
        pa.Table.from_pylist([], schema=schema),
        OUT / table / HEADER,
        compression="snappy",
    )


def local_rows(table: str) -> tuple[int, int]:
    files = sorted((OUT / table).rglob("*.parquet"))
    return sum(pq.ParquetFile(f).metadata.num_rows for f in files), len(files)


def credentials_path() -> str:
    """Path to the service-account key basedosdados is configured to use.

    Read from ~/.basedosdados/config.toml so gcloud and the python client cannot
    drift onto different identities.  The file itself is never opened here, only
    its path handed to gcloud.
    """
    import tomli

    config = tomli.loads(
        (Path.home() / ".basedosdados/config.toml").read_text()
    )
    return config["gcloud-projects"]["staging"]["credentials_path"]


def rsync_to_gcs(table: str) -> None:
    source = str(OUT / table)
    destination = f"gs://{BILLING_PROJECT}/staging/{DATASET_ID}/{table}"
    print(f"[{table}] rsync -> {destination}", flush=True)
    subprocess.run(
        [
            "gcloud",
            "storage",
            "rsync",
            "-r",
            "--delete-unmatched-destination-objects",
            f"--billing-project={BILLING_PROJECT}",
            source,
            destination,
        ],
        check=True,
        env={
            **os.environ,
            "CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE": credentials_path(),
        },
    )


def create_external_table(table: str) -> None:
    bd.Table(dataset_id=DATASET_ID, table_id=table).create(
        path=str(OUT / table),
        source_format="parquet",
        if_table_exists="replace",
        if_storage_data_exists="pass",  # already uploaded by rsync
        if_dataset_exists="pass",
    )


def bq_rows(table: str) -> int:
    frame = bd.read_sql(
        f"select count(*) as n from `{BILLING_PROJECT}.{DATASET_ID}_staging.{table}`",
        billing_project_id=BILLING_PROJECT,
        from_file=True,
    )
    return int(frame["n"].iloc[0])


def upload(table: str) -> None:
    if not (OUT / table).exists():
        raise SystemExit(f"{table}: nothing cleaned at {OUT / table}")

    write_header_stub(table)
    expected, files = local_rows(table)
    print(f"[{table}] local: {expected:,} rows in {files} file(s)", flush=True)

    rsync_to_gcs(table)
    create_external_table(table)

    actual = bq_rows(table)
    if actual != expected:
        raise SystemExit(
            f"{table}: BigQuery has {actual:,} rows, local parquet has "
            f"{expected:,}. Stopping before the next table."
        )
    print(f"[{table}] OK: {actual:,} rows in BigQuery", flush=True)


if __name__ == "__main__":
    for name in sys.argv[1:] or TABLES:
        upload(name)
