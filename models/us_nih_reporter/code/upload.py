"""Upload the cleaned us_nih_reporter parquet to BigQuery staging (basedosdados-dev).

    python upload.py                    # every table
    python upload.py project dicionario # selected tables

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
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT))

import google.cloud.storage as gcs  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402
from common import (  # noqa: E402
    ALL_TABLES,
    DATASET_ID,
    OUTPUT,
    PARTITIONED_TABLES,
)
from google.cloud import bigquery  # noqa: E402

BILLING_PROJECT = "basedosdados-dev"
BUCKET = "basedosdados-dev"

# One parquet file per year for the partitioned tables; one file for the rest.
# Projects and abstracts span FY1985-FY2025 (41 partitions); publications and
# their link tables span CY1980-CY2025 (46 partitions).
EXPECTED_FILES = {
    "project": 41,
    "project_abstract": 41,
    "publication": 46,
    "publication_link": 46,
    "patent_link": 1,
    "clinical_study_link": 1,
    "dicionario": 1,
}

_orig_bucket = gcs.Client.bucket


def _patched_bucket(
    self: gcs.Client, bucket_name: str, user_project: str | None = None
) -> gcs.Bucket:
    """Return a bucket handle that always bills BILLING_PROJECT.

    The Data Basis buckets are requester-pays, so every call needs a
    user_project. Callers inside the basedosdados SDK do not pass one, hence
    the patch rather than a wrapper.

    Args:
        self: The storage client, since this replaces an unbound method.
        bucket_name: Name of the bucket to open.
        user_project: Ignored — BILLING_PROJECT always wins.

    Returns:
        The bucket handle, pinned to the billing project.
    """
    return _orig_bucket(self, bucket_name, user_project=BILLING_PROJECT)


gcs.Client.bucket = _patched_bucket

from pipelines.utils.tasks import _upload_to_gcs  # noqa: E402


def local_rows(table: str) -> tuple[int, int]:
    """Row count and file count of a table's local parquet.

    Args:
        table: Clean table slug.

    Returns:
        ``(rows, files)`` read from the parquet metadata, not the data.
    """
    files = sorted((OUTPUT / table).rglob("*.parquet"))
    return sum(pq.ParquetFile(f).metadata.num_rows for f in files), len(files)


def staging_rows(client: bigquery.Client, table: str) -> int:
    """Row count of the table's staging external table.

    Args:
        client: An authenticated BigQuery client.
        table: Clean table slug.

    Returns:
        The row count reported by BigQuery.
    """
    ref = f"{BILLING_PROJECT}.{DATASET_ID}_staging.{table}"
    return next(
        iter(client.query(f"select count(*) n from `{ref}`").result())
    ).n


def upload(table: str) -> None:
    """Upload one table's parquet to staging and verify what landed.

    Args:
        table: Clean table slug.

    Returns:
        None.

    Raises:
        SystemExit: when the parquet file count, the staging row count, the
            table type or the staging schema does not match what the pipeline
            will later expect.
    """
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
    # minutes into a dbt build.
    typed = [f.name for f in ref.schema if f.field_type != "STRING"]
    if typed:
        raise SystemExit(
            f"[{table}] staging schema is not all-STRING: {typed} — drop the "
            "staging table and its GCS prefix, then re-upload"
        )
    if table in PARTITIONED_TABLES and "year" not in [
        f.name for f in ref.schema
    ]:
        raise SystemExit(
            f"[{table}] staging schema has no year column — the partition key "
            "must be a real column, not only the hive directory name"
        )
    print(f"[{table}] OK")


def main() -> None:
    """Upload every table named on the command line, or all of them.

    Returns:
        None.

    Raises:
        SystemExit: when credentials are unset or a table name is unknown.
    """
    if not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        raise SystemExit("GOOGLE_APPLICATION_CREDENTIALS is not set")
    # gcs.dump_header writes its 0-row header parquet to ./data/<uuid>/ relative
    # to the working directory. Run from a temp directory so that scratch does
    # not land in the repo; every path this script passes is absolute.
    os.chdir(tempfile.mkdtemp(prefix="us_nih_reporter_upload_"))
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
