"""Upload the cleaned us_epa_ghgrp parquet tables to BigQuery.

Usage:
    uv run python models/us_epa_ghgrp/code/upload.py [--env dev|prod] [table_slug ...]

--env dev (default) -> basedosdados-dev; --env prod -> basedosdados. Onboarding
only ever uploads to dev: the prod tables are materialised by the table-approve
action when the PR merges. Uploads smallest first and stops on the first failure.

Reads the parquet written by clean_data.py under ``~/Downloads/us_epa_ghgrp_data``
(override with ``EPA_GHGRP_DATA_DIR``). The expected row count of each table is
read from the parquet metadata, so nothing is hardcoded here.
"""

import os
import sys
import warnings
from pathlib import Path

warnings.filterwarnings("ignore")

import basedosdados as bd  # noqa: E402
import google.cloud.storage as gcs  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402
from google.cloud import bigquery  # noqa: E402

from pipelines.datasets.us_epa_ghgrp.constants import constants  # noqa: E402

_argv = sys.argv[1:]
if "--env" in _argv:
    _i = _argv.index("--env")
    ENV = _argv[_i + 1]
    _argv = _argv[:_i] + _argv[_i + 2 :]
else:
    ENV = "dev"

BILLING_PROJECT = "basedosdados" if ENV == "prod" else "basedosdados-dev"
DATASET_ID = constants.DATASET_ID.value
OUTPUT_ROOT = (
    Path(
        os.environ.get(
            "EPA_GHGRP_DATA_DIR",
            Path.home() / "Downloads" / "us_epa_ghgrp_data",
        )
    )
    / "output"
)

# The GCS bucket is requester-pays.
_orig_bucket = gcs.Client.bucket


def _patched_bucket(self, bucket_name, user_project=None):
    return _orig_bucket(self, bucket_name, user_project=BILLING_PROJECT)


gcs.Client.bucket = _patched_bucket


def local_rows(path: Path) -> int:
    return sum(
        pq.ParquetFile(f).metadata.num_rows for f in path.rglob("*.parquet")
    )


def upload_table(slug: str) -> int:
    path = OUTPUT_ROOT / slug
    if not path.exists():
        raise FileNotFoundError(f"Missing output path: {path}")
    expected_rows = local_rows(path)

    # Delete the stale GCS staging prefix first — leftover files under a
    # different partition layout make BigQuery reject the external table.
    st = bd.Storage(dataset_id=DATASET_ID, table_id=slug)
    try:
        st.delete_table(mode="staging", not_found_ok=True)
    except Exception as e:
        print(f"  [warn] staging prefix cleanup: {e}")

    tb = bd.Table(dataset_id=DATASET_ID, table_id=slug)
    tb.create(
        path=str(path),
        source_format="parquet",
        if_table_exists="replace",
        if_storage_data_exists="replace",
        if_dataset_exists="pass",
    )

    client = bigquery.Client(project=BILLING_PROJECT)
    q = f"select count(*) as n from `{BILLING_PROJECT}.{DATASET_ID}_staging.{slug}`"
    n = next(iter(client.query(q).result())).n
    status = "OK" if n == expected_rows else "ROW MISMATCH"
    print(
        f"  {slug}: uploaded {n:,} rows (expected {expected_rows:,}) — {status}",
        flush=True,
    )
    if n != expected_rows:
        raise ValueError(
            f"{slug}: row count {n:,} != expected {expected_rows:,}"
        )
    return n


def main() -> None:
    only = set(_argv)
    tables = [t for t in constants.TABLES.value if not only or t in only]
    tables.sort(key=lambda t: local_rows(OUTPUT_ROOT / t))
    print(f"=== uploading to {BILLING_PROJECT} (env={ENV}) ===", flush=True)
    for slug in tables:
        print(f"=== {slug} ===", flush=True)
        try:
            upload_table(slug)
        except Exception as e:
            print(f"  FAILED: {type(e).__name__}: {e}")
            sys.exit(1)
    print("ALL TABLES UPLOADED")


if __name__ == "__main__":
    main()
