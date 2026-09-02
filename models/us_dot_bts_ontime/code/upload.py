"""Upload the cleaned parquet of us_dot_bts_ontime to BigQuery dev staging.

    uv run --no-project --with basedosdados --with pyarrow --with tomli \
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
import subprocess
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

# `flight` is ~11 GB across 465 partitioned files. bd.Table.create uploads through a
# single-threaded python resumable session, which does not reliably survive a
# transfer that size; rsync is parallel, retries internally, and skips blobs already
# present, so a failure costs only the file it was on.
RSYNC_TABLES = {"flight"}
RSYNC_RESUME = os.environ.get("BTS_RSYNC_RESUME") == "1"

_orig_bucket = gcs.Client.bucket


def _patched_bucket(self, bucket_name, user_project=None):
    return _orig_bucket(self, bucket_name, user_project=BILLING_PROJECT)


gcs.Client.bucket = _patched_bucket


def _credentials_path() -> str:
    """Path to the service-account key basedosdados is configured to use.

    Read from ~/.basedosdados/config.toml so gcloud and the python client cannot
    drift onto different identities. The file itself is never opened here — only its
    path is passed to gcloud.
    """
    import tomli

    cfg = tomli.loads(
        (Path.home() / ".basedosdados" / "config.toml").read_text()
    )
    return cfg["gcloud-projects"]["staging"]["credentials_path"]


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


def rsync_to_gcs(table: str) -> None:
    """Push a table's parquet tree to its staging prefix with `gcloud storage rsync`.

    `gcloud storage`, not `gsutil`: gsutil still carries Python 2 code and dies on
    these files with `module 'sys' has no attribute 'maxint'`.

    The blob layout is identical to bd's — staging/<ds>/<table>/year=YYYY/... — so the
    external table cannot tell which path uploaded it.
    """
    src = str(OUTPUT / table)
    dest = f"gs://{BILLING_PROJECT}/staging/{DATASET_ID}/{table}"
    print(f"[{table}] rsync {src} -> {dest}")
    env = {
        **os.environ,
        # Drive gcloud with the same service account the rest of this script uses,
        # rather than whatever user credentials happen to be cached. Those expire,
        # and when they do gcloud fails with "Reauthentication failed. cannot prompt
        # during non-interactive execution", which a script cannot recover from.
        "CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE": _credentials_path(),
    }
    subprocess.run(
        [
            "gcloud",
            "storage",
            "rsync",
            "-r",
            f"--billing-project={BILLING_PROJECT}",
            src,
            dest,
        ],
        check=True,
        env=env,
    )


def upload_table(table: str) -> None:
    path = OUTPUT / table
    if not path.exists():
        raise SystemExit(
            f"{table}: nothing cleaned at {path} — run clean.py first"
        )

    expected, nfiles = local_rows(table)
    print(f"[{table}] local: {expected:,} rows in {nfiles} parquet file(s)")

    storage = bd.Storage(dataset_id=DATASET_ID, table_id=table)
    bq_table = bd.Table(dataset_id=DATASET_ID, table_id=table)

    if table in RSYNC_TABLES:
        # Clear the prefix first. rsync only adds and overwrites; it does not remove
        # objects that no longer exist locally, so skipping this would leave a stale
        # partition layout alongside the new one and the external table would read
        # both — silently double-counting. BTS_RSYNC_RESUME=1 skips the delete so an
        # interrupted transfer resumes; only safe when the local layout has not
        # changed. The row-count check below is the backstop either way.
        if not RSYNC_RESUME:
            storage.delete_table(mode="staging", not_found_ok=True)
        rsync_to_gcs(table)
        # Schema comes from the 0-row 00_header.parquet, so this never reads a large
        # file into pandas.
        bq_table.create(
            path=str(path),
            source_format="parquet",
            if_table_exists="replace",
            if_storage_data_exists="pass",  # already uploaded by the rsync
            if_dataset_exists="pass",
        )
    else:
        # Clear the staging prefix first: leftover objects from an earlier shape
        # produce BigQuery partition-key conflicts on the external table.
        storage.delete_table(mode="staging", not_found_ok=True)
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
        f"\nall requested tables uploaded to {BILLING_PROJECT}.{DATASET_ID}_staging"
    )


if __name__ == "__main__":
    main(sys.argv[1:])
