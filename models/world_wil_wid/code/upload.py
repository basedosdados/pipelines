"""Upload the cleaned world_wil_wid parquet tables to BigQuery staging.

Usage::

    uv run python models/world_wil_wid/code/upload.py [--env dev|prod] [table ...]

``--env dev`` (the default) targets ``basedosdados-dev``; ``--env prod`` targets
``basedosdados``. Point ``GOOGLE_APPLICATION_CREDENTIALS`` at the matching
service account. Reads the cleaned parquet from ``$WID_DATA_DIR/output``
(default ``~/Downloads/world_wil_wid_data/output``), which is never under the
repo or Dropbox.

The expected row count per table is read from the parquet footers rather than
hardcoded, so a re-clean cannot silently drift from a stale constant. Tables go
up smallest first and the run stops on the first failure.
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

_argv = sys.argv[1:]
if "--env" in _argv:
    _i = _argv.index("--env")
    ENV = _argv[_i + 1]
    _argv = _argv[:_i] + _argv[_i + 2 :]
else:
    ENV = "dev"
BILLING_PROJECT = "basedosdados" if ENV == "prod" else "basedosdados-dev"
DATASET_ID = "world_wil_wid"
DATA_ROOT = Path(
    os.environ.get(
        "WID_DATA_DIR", Path.home() / "Downloads" / "world_wil_wid_data"
    )
)
OUTPUT_ROOT = DATA_ROOT / "output"

# Smallest first, so a credentials or convention problem surfaces on a 400-row
# table rather than after pushing 142M rows.
TABLES = ["dicionario", "country", "series", "indicator"]

_orig_bucket = gcs.Client.bucket


def _patched_bucket(
    self: gcs.Client,
    bucket_name: str,
    user_project: str | None = None,
    generation: int | None = None,
) -> "gcs.Bucket":
    """Force ``user_project`` so the requester-pays bucket bills our project.

    Mirrors ``gcs.Client.bucket``'s full signature (including ``generation``)
    so callers passing a bucket generation keep working.

    Args:
        self: The storage client the method is bound to.
        bucket_name: Name of the bucket to instantiate.
        user_project: Ignored; overridden with the billing project.
        generation: Optional bucket generation, passed through unchanged.

    Returns:
        The instantiated ``Bucket`` billed to ``BILLING_PROJECT``.
    """
    return _orig_bucket(
        self,
        bucket_name,
        user_project=BILLING_PROJECT,
        generation=generation,
    )


gcs.Client.bucket = _patched_bucket


def local_rows(path: Path) -> int:
    """Count rows across a table's parquet files without reading their data.

    Args:
        path: The table's output directory.

    Returns:
        Total row count from the parquet footers.
    """
    return sum(
        pq.ParquetFile(file).metadata.num_rows
        for file in sorted(path.rglob("*.parquet"))
    )


def upload_table(slug: str) -> int:
    """Upload one cleaned parquet table to BigQuery staging.

    Deletes any stale staging prefix first -- leaving it in place makes BigQuery
    fail on conflicting partition keys -- then recreates the staging table and
    asserts the loaded row count matches the local parquet.

    Args:
        slug: Table slug, also the output subdirectory name.

    Returns:
        The number of rows loaded into the staging table.

    Raises:
        FileNotFoundError: If the table's output directory is missing.
        SystemExit: If the loaded row count differs from the local count.
    """
    path = OUTPUT_ROOT / slug
    if not path.exists():
        raise FileNotFoundError(f"Missing output path: {path}")
    expected = local_rows(path)

    storage = bd.Storage(dataset_id=DATASET_ID, table_id=slug)
    try:
        storage.delete_table(mode="staging", not_found_ok=True)
    except Exception as error:
        print(f"  [warn] staging prefix cleanup: {error}")

    table = bd.Table(dataset_id=DATASET_ID, table_id=slug)
    table.create(
        path=str(path),
        source_format="parquet",
        if_table_exists="replace",
        if_storage_data_exists="replace",
        if_dataset_exists="pass",
    )

    client = bigquery.Client(project=BILLING_PROJECT)
    query = (
        "select count(*) as n from "
        f"`{BILLING_PROJECT}.{DATASET_ID}_staging.{slug}`"
    )
    loaded = next(iter(client.query(query).result())).n
    if loaded != expected:
        raise SystemExit(
            f"  {slug}: uploaded {loaded:,} rows but the local parquet holds "
            f"{expected:,} -- aborting"
        )
    print(f"  {slug}: uploaded {loaded:,} rows -- OK")
    return loaded


def main() -> None:
    """Upload each table in ``TABLES`` (smallest first), stopping on failure."""
    targets = _argv or TABLES
    print(f"env={ENV} billing={BILLING_PROJECT} dataset={DATASET_ID}")
    for slug in TABLES:
        if slug in targets:
            print(f"[upload] {slug}")
            upload_table(slug)
    print("done.")


if __name__ == "__main__":
    main()
