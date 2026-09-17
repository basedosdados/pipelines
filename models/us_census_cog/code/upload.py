"""Upload the cleaned us_census_cog parquet to BigQuery staging (basedosdados-dev).

    python upload.py                       # every table
    python upload.py finance government_unit

Uses ``pipelines.utils.tasks._upload_to_gcs`` -- the same helper the recurring
flow calls -- rather than ``bd.Table.create(path=<data>)`` or a BigQuery load
job. That matters twice over:

* ``_upload_to_gcs`` hands ``tb.create`` a 0-row header from ``dump_header`` and
  streams the data files separately, so RAM stays flat on the 90-million-row
  finance table.
* It leaves staging as an EXTERNAL table over
  ``gs://<bucket>/staging/<ds>/<tbl>/*``. A ``load_table_from_uri`` bootstrap
  would leave a NATIVE table instead, which silently ignores every file a later
  pipeline run writes -- dbt would keep serving this bootstrap snapshot forever
  with no error and no failing test.

``dump_mode="append"`` is deliberate: ``"overwrite"`` calls
``tb.delete(mode="all")``, which drops the production table too, even when
invoked against dev.

Requires GOOGLE_APPLICATION_CREDENTIALS pointing at a Data Basis dev
service-account key and ~/.basedosdados/config.toml. The bucket is
requester-pays, so ``gcs.Client.bucket`` is patched to pin ``user_project``.
"""

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT))

import google.cloud.storage as gcs  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402
from common import ALL_TABLES, DATASET_ID, OUTPUT  # noqa: E402

BILLING_PROJECT = "basedosdados-dev"
BUCKET = "basedosdados-dev"

_orig_bucket = gcs.Client.bucket


def _patched_bucket(
    self: gcs.Client, bucket_name: str, user_project: str | None = None
) -> gcs.Bucket:
    """Return a bucket handle with the billing project always pinned.

    The staging bucket is requester-pays, so every call needs a user project,
    and callers inside the basedosdados SDK do not pass one.

    Args:
        self: The storage client the method is bound to.
        bucket_name: Name of the bucket to open.
        user_project: Ignored; the billing project is pinned instead.

    Returns:
        The bucket, billed to BILLING_PROJECT.
    """
    return _orig_bucket(self, bucket_name, user_project=BILLING_PROJECT)


gcs.Client.bucket = _patched_bucket

from pipelines.utils.tasks import _upload_to_gcs  # noqa: E402


def summarise(table: str) -> tuple[int, int]:
    """Return the file and row counts written for a table."""
    root = OUTPUT / table
    files = sorted(root.rglob("data.parquet"))
    rows = sum(pq.ParquetFile(f).metadata.num_rows for f in files)
    return len(files), rows


def main(tables: list[str]) -> None:
    """Upload each requested table to staging."""
    for table in tables or list(ALL_TABLES):
        root = OUTPUT / table
        if not root.exists():
            raise SystemExit(f"{table}: nothing cleaned at {root}")
        files, rows = summarise(table)
        print(f"{table}: uploading {files} file(s), {rows:,} rows", flush=True)
        _upload_to_gcs(
            data_path=str(root),
            dataset_id=DATASET_ID,
            table_id=table,
            bucket_name=BUCKET,
            dump_mode="append",
            source_format="parquet",
        )
        print(f"{table}: done", flush=True)


if __name__ == "__main__":
    main(sys.argv[1:])
