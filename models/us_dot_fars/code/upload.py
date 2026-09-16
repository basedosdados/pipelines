"""Upload the cleaned us_dot_fars parquet to BigQuery staging (basedosdados-dev).

    python upload.py                    # every table
    python upload.py crash person       # selected tables

Uses ``pipelines.utils.tasks._upload_to_gcs`` — the same helper the recurring flow
calls — rather than ``bd.Table.create(path=<data>)`` or a BigQuery load job. That
matters twice over:

* ``_upload_to_gcs`` hands ``tb.create`` a 0-row header from ``dump_header`` and
  streams the data files separately, so RAM stays flat.
* It leaves staging as an **EXTERNAL** table over ``gs://<bucket>/staging/<ds>/<tbl>/*``.
  A ``load_table_from_uri`` bootstrap would leave a NATIVE table instead, which
  silently ignores every file a later pipeline run writes — dbt would keep serving
  this bootstrap snapshot forever with no error and no failing test.

``dump_mode="append"`` is deliberate: ``"overwrite"`` calls ``tb.delete(mode="all")``,
which drops the production table too, even when invoked against dev.

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
from common import ALL_TABLES, DATASET_ID, OUTPUT  # noqa: E402
from google.cloud import bigquery  # noqa: E402

BILLING_PROJECT = "basedosdados-dev"
BUCKET = "basedosdados-dev"

# The three data tables are hive-partitioned by year over 1975..2024, one file
# per year with no gaps; dicionario is a single file.
EXPECTED_FILES = {
    "crash": 50,
    "vehicle": 50,
    "person": 50,
    "dicionario": 1,
}

_orig_bucket = gcs.Client.bucket


def _patched_bucket(self, bucket_name, user_project=None):
    return _orig_bucket(self, bucket_name, user_project=BILLING_PROJECT)


gcs.Client.bucket = _patched_bucket

from pipelines.utils.tasks import _upload_to_gcs  # noqa: E402


def local_rows(table: str) -> tuple[int, int]:
    files = sorted((OUTPUT / table).rglob("*.parquet"))
    return sum(pq.ParquetFile(f).metadata.num_rows for f in files), len(files)


def staging_rows(client: bigquery.Client, table: str) -> int:
    ref = f"{BILLING_PROJECT}.{DATASET_ID}_staging.{table}"
    return next(
        iter(client.query(f"select count(*) n from `{ref}`").result())
    ).n


def upload(table: str) -> None:
    expected, nfiles = local_rows(table)
    print(
        f"\n[{table}] local: {expected:,} rows across {nfiles} parquet file(s)"
    )
    if nfiles != EXPECTED_FILES[table]:
        raise SystemExit(
            f"[{table}] expected {EXPECTED_FILES[table]} parquet file(s), found "
            f"{nfiles} — finish clean.py first"
        )

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
    # model's safe_cast never gets the chance to run. Caught here rather than
    # four minutes into a dbt build.
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
