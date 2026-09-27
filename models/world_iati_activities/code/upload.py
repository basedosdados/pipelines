"""Upload the cleaned world_iati_activities parquet to BigQuery staging (basedosdados-dev).

    python upload.py                     # every table
    python upload.py activity transaction

Uses ``pipelines.utils.tasks._upload_to_gcs`` — the same helper the recurring flow
calls — rather than ``bd.Table.create(path=<data>)`` or a BigQuery load job. That
matters twice over:

* ``_upload_to_gcs`` hands ``tb.create`` a 0-row header from ``dump_header`` and
  streams the data files separately, so RAM stays flat. transaction_breakdown is
  24.6M rows; reading it into pandas would not fit.
* It leaves staging as an **EXTERNAL** table over ``gs://<bucket>/staging/<ds>/<tbl>/*``.
  A ``load_table_from_uri`` bootstrap would leave a NATIVE table instead, which
  silently ignores every file a later pipeline run writes — dbt would keep serving
  this bootstrap snapshot forever with no error and no failing test.

``dump_mode="append"`` is deliberate: ``"overwrite"`` calls ``tb.delete(mode="all")``,
which drops the production table too, even when invoked against dev. The stale GCS
prefix is cleared first instead, so a partition that disappears between runs does
not linger.

Requires GOOGLE_APPLICATION_CREDENTIALS pointing at a Data Basis dev service-account
key and ~/.basedosdados/config.toml. The bucket is requester-pays, so
``gcs.Client.bucket`` is patched to pin ``user_project`` to the billing project.
"""

import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT))

import google.cloud.storage as gcs  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402
from common import OUTPUT  # noqa: E402
from google.cloud import bigquery  # noqa: E402

from pipelines.datasets.world_iati_activities.constants import (  # noqa: E402
    constants,
)

BILLING_PROJECT = "basedosdados-dev"
BUCKET = "basedosdados-dev"
DATASET_ID = constants.DATASET_ID.value
ALL_TABLES = constants.ALL_TABLES.value

_orig_bucket = gcs.Client.bucket


def _patched_bucket(self, bucket_name, user_project=None):
    return _orig_bucket(self, bucket_name, user_project=BILLING_PROJECT)


gcs.Client.bucket = _patched_bucket

from pipelines.utils.tasks import _upload_to_gcs  # noqa: E402


def local_rows(table: str) -> tuple[int, int]:
    files = sorted((OUTPUT / table).rglob("*.parquet"))
    return sum(pq.ParquetFile(f).metadata.num_rows for f in files), len(files)


def clear_staging_prefix(table: str) -> int:
    """Remove the previous run's files. Without this a partition that vanished
    between runs stays readable through the external table, and BigQuery reports
    a partition-key conflict when the layout changes."""
    client = gcs.Client(project=BILLING_PROJECT)
    bucket = client.bucket(BUCKET)
    blobs = list(bucket.list_blobs(prefix=f"staging/{DATASET_ID}/{table}/"))
    for blob in blobs:
        blob.delete()
    return len(blobs)


def staging_rows(client: bigquery.Client, table: str) -> int:
    ref = f"{BILLING_PROJECT}.{DATASET_ID}_staging.{table}"
    return next(
        iter(client.query(f"select count(*) n from `{ref}`").result())
    ).n


def upload(table: str) -> None:
    expected, nfiles = local_rows(table)
    if not nfiles:
        raise SystemExit(f"[{table}] no parquet under {OUTPUT / table}")
    print(
        f"\n[{table}] local: {expected:,} rows across {nfiles} parquet file(s)"
    )

    removed = clear_staging_prefix(table)
    if removed:
        print(f"[{table}] cleared {removed} stale object(s) from GCS")

    _upload_to_gcs(
        data_path=OUTPUT / table,
        dataset_id=DATASET_ID,
        table_id=table,
        bucket_name=BUCKET,
        dump_mode="append",
        source_format="parquet",
    )

    client = bigquery.Client(project=BILLING_PROJECT)
    ref = client.get_table(f"{BILLING_PROJECT}.{DATASET_ID}_staging.{table}")
    got = staging_rows(client, table)
    print(f"[{table}] staging: {got:,} rows, table_type={ref.table_type}")
    if ref.table_type != "EXTERNAL":
        raise SystemExit(
            f"[{table}] staging table is {ref.table_type}, expected EXTERNAL — a "
            "native table ignores the files a later pipeline run writes"
        )
    if got != expected:
        raise SystemExit(
            f"[{table}] ROW COUNT MISMATCH: {got:,} != {expected:,}"
        )

    # Staging must be all-STRING. dump_header infers the schema from the first
    # parquet file it finds, and an unlucky one — a zero-row partition, or a
    # column whose values all happen to look numeric — makes BigQuery autodetect
    # a non-string type. Every real partition then fails to read with "has type
    # BYTE_ARRAY which does not match the target cpp_type INT64", and the dbt
    # model's safe_cast never gets the chance to run.
    typed = [f.name for f in ref.schema if f.field_type != "STRING"]
    if typed:
        raise SystemExit(
            f"[{table}] staging schema is not all-STRING: {typed} — drop the "
            "staging table and its GCS prefix, then re-upload"
        )
    print(f"[{table}] OK")


def main() -> None:
    if not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        raise SystemExit("GOOGLE_APPLICATION_CREDENTIALS is not set")
    tables = sys.argv[1:] or ALL_TABLES
    for t in tables:
        if t not in ALL_TABLES:
            raise SystemExit(
                f"unknown table {t!r}; expected one of {ALL_TABLES}"
            )
        upload(t)
    print("\nall uploads verified")


if __name__ == "__main__":
    main()
