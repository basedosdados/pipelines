"""Upload the cleaned world_oecd_education parquet to BigQuery staging (dev).

    python upload.py                 # every table
    python upload.py student finance # selected tables

Uses ``pipelines.utils.tasks._upload_to_gcs`` rather than a BigQuery load job or
``bd.Table.create(path=<data>)``, for two reasons that have each cost a debugging
session elsewhere in this repo:

* it hands ``tb.create`` a 0-row header from ``dump_header`` and streams the data
  files separately, so RAM stays flat -- ``finance`` is 8.1M rows;
* it leaves staging as an **EXTERNAL** table over the GCS prefix. A load job would
  leave a NATIVE table, which silently ignores every file written later, so dbt
  would serve this one snapshot forever with no error and no failing test.

``dump_mode="append"`` is deliberate: ``"overwrite"`` calls ``tb.delete(mode="all")``,
which drops the *production* table too, even when invoked against dev.

Requires GOOGLE_APPLICATION_CREDENTIALS pointing at a dev service-account key and
~/.basedosdados/config.toml. The bucket is requester-pays, so ``gcs.Client.bucket``
is patched to pin ``user_project``.
"""

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT))

import google.cloud.storage as gcs  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402
from common import DATASET_ID, OUTPUT  # noqa: E402
from google.cloud import bigquery  # noqa: E402
from tables import TABLES  # noqa: E402

BILLING_PROJECT = "basedosdados-dev"
BUCKET = "basedosdados-dev"

_orig_bucket = gcs.Client.bucket


def _patched_bucket(self, bucket_name, user_project=None):
    """Return a bucket handle pinned to the billing project (requester-pays)."""
    return _orig_bucket(self, bucket_name, user_project=BILLING_PROJECT)


gcs.Client.bucket = _patched_bucket

from pipelines.utils.tasks import _upload_to_gcs  # noqa: E402


def local(table):
    """(rows, files) of a table's cleaned parquet, from the footers."""
    files = sorted((OUTPUT / table).rglob("*.parquet"))
    return sum(pq.ParquetFile(f).metadata.num_rows for f in files), len(files)


def upload(table):
    """Upload one table and verify what actually landed in staging."""
    expected, nfiles = local(table)
    if not nfiles:
        raise SystemExit(f"[{table}] no parquet found -- run clean.py first")
    print(f"\n[{table}] local: {expected:,} rows in {nfiles} file(s)")

    _upload_to_gcs(
        data_path=OUTPUT / table,
        dataset_id=DATASET_ID,
        table_id=table,
        bucket_name=BUCKET,
        dump_mode="append",
        source_format="parquet",
    )

    client = bigquery.Client(project=BILLING_PROJECT)
    ref = f"{BILLING_PROJECT}.{DATASET_ID}_staging.{table}"
    meta = client.get_table(ref)
    got = next(
        iter(client.query(f"select count(*) n from `{ref}`").result())
    ).n
    print(f"[{table}] staging: {got:,} rows, table_type={meta.table_type}")

    if meta.table_type != "EXTERNAL":
        raise SystemExit(
            f"[{table}] staging is {meta.table_type}, expected EXTERNAL -- a native "
            "table ignores files written later"
        )
    if got != expected:
        raise SystemExit(
            f"[{table}] staging has {got:,} rows, local has {expected:,}"
        )
    typed = [f.name for f in meta.schema if f.field_type != "STRING"]
    if typed:
        raise SystemExit(
            f"[{table}] staging schema is not all-STRING: {typed} -- the dbt model "
            "safe_casts every column, and a typed staging column fails on first read"
        )
    print(f"[{table}] OK")


def main():
    for table in sys.argv[1:] or list(TABLES):
        upload(table)


if __name__ == "__main__":
    main()
