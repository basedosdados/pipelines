"""Upload cleaned us_fed_fred parquet tables to BigQuery staging.

Usage:
    uv run python models/us_fed_fred/code/upload.py [--env dev|prod] [table_slug ...]

--env dev (default) -> basedosdados-dev; --env prod -> basedosdados. Point
GOOGLE_APPLICATION_CREDENTIALS at the matching service account. Reads the cleaned
parquet from $US_FED_FRED_DATA/output (default ~/Downloads/us_fed_fred_data/output),
which is NEVER under the repo or Dropbox. Uploads smallest table first and stops on
first failure.
"""

import os
import sys
import warnings
from pathlib import Path

warnings.filterwarnings("ignore")

import basedosdados as bd  # noqa: E402
import google.cloud.storage as gcs  # noqa: E402
from google.cloud import bigquery  # noqa: E402

_argv = sys.argv[1:]
if "--env" in _argv:
    _i = _argv.index("--env")
    ENV = _argv[_i + 1]
    _argv = _argv[:_i] + _argv[_i + 2 :]
else:
    ENV = "dev"
BILLING_PROJECT = "basedosdados" if ENV == "prod" else "basedosdados-dev"
DATASET_ID = "us_fed_fred"
DATA_ROOT = Path(
    os.environ.get(
        "US_FED_FRED_DATA", Path.home() / "Downloads" / "us_fed_fred_data"
    )
)
OUTPUT_ROOT = DATA_ROOT / "output"

# Monkey-patch for requester-pays bucket
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

# (table_slug, expected_rows) — smallest first
TABLES = [
    ("series", 50),
    ("observation", 171_801),
]


def upload_table(slug: str, expected_rows: int) -> int:
    """Upload one cleaned parquet table to BigQuery staging.

    Deletes any stale staging prefix, recreates the staging table from the
    parquet under ``OUTPUT_ROOT/slug``, then asserts the loaded row count
    matches ``expected_rows``.

    Args:
        slug: Table slug (also the output subdirectory name).
        expected_rows: Row count the upload must produce.

    Returns:
        The number of rows loaded into the staging table.

    Raises:
        FileNotFoundError: If the table's output directory is missing.
        SystemExit: If the loaded row count differs from ``expected_rows``.
    """
    path = OUTPUT_ROOT / slug
    if not path.exists():
        raise FileNotFoundError(f"Missing output path: {path}")

    tb = bd.Table(dataset_id=DATASET_ID, table_id=slug)

    st = bd.Storage(dataset_id=DATASET_ID, table_id=slug)
    try:
        st.delete_table(mode="staging", not_found_ok=True)
    except Exception as e:
        print(f"  [warn] staging prefix cleanup: {e}")

    tb.create(
        path=str(path),
        source_format="parquet",
        if_table_exists="replace",
        if_storage_data_exists="replace",
        if_dataset_exists="pass",
    )

    client = bigquery.Client(project=BILLING_PROJECT)
    q = (
        "select count(*) as n from "
        f"`{BILLING_PROJECT}.{DATASET_ID}_staging.{slug}`"
    )
    n = next(iter(client.query(q).result())).n
    if n != expected_rows:
        raise SystemExit(
            f"  {slug}: uploaded {n:,} rows but expected "
            f"{expected_rows:,} — aborting"
        )
    print(f"  {slug}: uploaded {n:,} rows (expected {expected_rows:,}) — OK")
    return n


def main() -> None:
    """Upload each table in ``TABLES`` (smallest first), stopping on failure."""
    targets = _argv or [t[0] for t in TABLES]
    print(f"env={ENV} billing={BILLING_PROJECT} dataset={DATASET_ID}")
    for slug, expected in TABLES:
        if slug in targets:
            print(f"[upload] {slug}")
            upload_table(slug, expected)
    print("done.")


if __name__ == "__main__":
    main()
