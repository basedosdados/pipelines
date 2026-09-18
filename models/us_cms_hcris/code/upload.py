"""Upload the cleaned us_cms_hcris parquet to BigQuery staging (basedosdados-dev).

    python upload.py                 # every staged table
    python upload.py report          # one table

Uses ``pipelines.utils.tasks._upload_to_gcs`` — the same helper the recurring
flow calls — rather than ``bd.Table.create(path=<data>)`` or a BigQuery load
job. That matters twice over:

* ``_upload_to_gcs`` hands ``tb.create`` a 0-row header from ``dump_header`` and
  streams the data files separately, so RAM stays flat.
* It leaves staging as an **EXTERNAL** table over
  ``gs://<bucket>/staging/<ds>/<tbl>/*``. A ``load_table_from_uri`` bootstrap
  would leave a NATIVE table instead, which silently ignores every file a later
  pipeline run writes — dbt would keep serving this bootstrap snapshot forever
  with no error and no failing test.

``dump_mode="append"`` is deliberate: ``"overwrite"`` calls
``tb.delete(mode="all")``, which drops the **production** table too, even when
invoked against dev.

**No ``00_header.parquet`` is seeded here, and it must not be.** That trick
exists so the table-approve action's ``save_header_files`` reads a 0-row file
rather than loading a huge first partition into pandas and OOM-ing the runner.
Two things rule it out for this dataset. The staging external table is
**hive-partitioned** on ``year``, so a file at the prefix root has no partition
key and BigQuery rejects every read of the table with "Incompatible partition
schemas. Expected schema ([year:TYPE_STRING]) has 1 columns. Observed schema
([]) has 0 columns" — measured here, not assumed. And it is not needed: the
lexicographically first blob is ``year=1996/...``, 7.4 million rows and about
40 MB, well inside the range that runner handles (105M rows / 161 MB is known
to succeed). Revisit only if a future partition lands before 1996.

Only ``report`` and ``report_value`` are uploaded. ``hospital_financial`` and
``dicionario`` are dbt models built from them, so they have no staging table.

Requires GOOGLE_APPLICATION_CREDENTIALS pointing at a Data Basis dev
service-account key and ~/.basedosdados/config.toml. The buckets are
requester-pays, so ``gcs.Client.bucket`` is patched to pin the billing project.
"""

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT))

import google.cloud.storage as gcs  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402
from common import DATASET_ID, OUTPUT, STAGED_TABLES  # noqa: E402
from google.cloud import bigquery  # noqa: E402

BILLING_PROJECT = "basedosdados-dev"
BUCKET = "basedosdados-dev"

_orig_bucket = gcs.Client.bucket


def _patched_bucket(
    self: gcs.Client, bucket_name: str, user_project: str | None = None
) -> "gcs.Bucket":
    """Return a bucket handle pinned to the billing project.

    The Data Basis buckets are requester-pays, so every request must name a
    project to bill. Patching the client is the least invasive way to apply
    that to the uploads the basedosdados package makes internally.

    Args:
        self: The storage client the method is bound to.
        bucket_name: Bucket to open.
        user_project: Ignored; BILLING_PROJECT always wins.

    Returns:
        The bucket, with ``user_project`` set to BILLING_PROJECT.
    """
    return _orig_bucket(self, bucket_name, user_project=BILLING_PROJECT)


gcs.Client.bucket = _patched_bucket

from pipelines.utils.tasks import _upload_to_gcs  # noqa: E402


def local_rows(table: str) -> tuple[int, int]:
    """Count the rows and files of a table's local parquet.

    Args:
        table: Table slug.

    Returns:
        ``(row_count, file_count)``, read from the parquet footers.
    """
    files = sorted((OUTPUT / table).rglob("*.parquet"))
    return sum(pq.ParquetFile(f).metadata.num_rows for f in files), len(files)


def staging_table(client: bigquery.Client, table: str) -> bigquery.Table:
    """Fetch the staging table's metadata.

    Args:
        client: An authenticated BigQuery client.
        table: Table slug.

    Returns:
        The BigQuery table object.
    """
    return client.get_table(f"{BILLING_PROJECT}.{DATASET_ID}_staging.{table}")


def upload(table: str) -> None:
    """Upload one table's parquet to staging and verify what landed.

    Args:
        table: Table slug.

    Raises:
        SystemExit: If the staging table is NATIVE rather than EXTERNAL, if its
            schema is not all-STRING, or if its row count does not match the
            local parquet.
    """
    rows, files = local_rows(table)
    print(f"{table}: {rows:,} rows in {files} files -> staging")

    # The data first, so the staging schema is inferred from a real file.
    _upload_to_gcs(
        data_path=OUTPUT / table,
        dataset_id=DATASET_ID,
        table_id=table,
        bucket_name=BUCKET,
        dump_mode="append",
        source_format="parquet",
    )
    client = bigquery.Client(project=BILLING_PROJECT)
    meta = staging_table(client, table)
    if meta.table_type != "EXTERNAL":
        raise SystemExit(
            f"{table}: staging is {meta.table_type}, not EXTERNAL — a NATIVE "
            "staging table ignores every file a later pipeline run writes"
        )
    non_string = [f.name for f in meta.schema if f.field_type != "STRING"]
    if non_string:
        raise SystemExit(
            f"{table}: staging schema is not all-STRING: {non_string}"
        )

    ref = f"{BILLING_PROJECT}.{DATASET_ID}_staging.{table}"
    got = next(
        iter(client.query(f"select count(*) n from `{ref}`").result())
    ).n
    if got != rows:
        raise SystemExit(
            f"{table}: staging has {got:,} rows, local parquet has {rows:,}"
        )
    print(f"{table}: EXTERNAL, all-STRING, {got:,} rows — matches local")


def main() -> None:
    """Upload the requested tables, stopping at the first failure."""
    tables = sys.argv[1:] or STAGED_TABLES
    unknown = set(tables) - set(STAGED_TABLES)
    if unknown:
        raise SystemExit(f"not staged tables: {sorted(unknown)}")
    for table in tables:
        upload(table)


if __name__ == "__main__":
    main()
