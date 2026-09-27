"""Upload cleaned au_aph_hansard parquet tables to BigQuery.

Usage:
    uv run python models/au_aph_hansard/code/upload.py [--env dev|prod] [table ...]

--env dev (default) -> basedosdados-dev; --env prod -> basedosdados. Point
GOOGLE_APPLICATION_CREDENTIALS at the matching service account. Uploads
smallest first and stops on the first failure.

Row counts are read from the local parquet footers rather than hardcoded, so
the check stays honest when the corpus is re-harvested.
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
DATASET_ID = "au_aph_hansard"
DATA_ROOT = Path(
    os.environ.get(
        "AU_APH_HANSARD_DATA",
        Path.home() / "Downloads" / "au_aph_hansard_data",
    )
)
OUTPUT_ROOT = DATA_ROOT / "output"

# The GCS bucket is requester-pays.
_orig_bucket = gcs.Client.bucket


def _patched_bucket(self, bucket_name, user_project=None):
    return _orig_bucket(self, bucket_name, user_project=BILLING_PROJECT)


gcs.Client.bucket = _patched_bucket

TABLES = ("dicionario", "sitting_day", "speech")


def local_rows(path: Path) -> int:
    """Row count across every parquet file under a table's output directory."""
    return sum(
        pq.ParquetFile(f).metadata.num_rows
        for f in sorted(path.rglob("*.parquet"))
    )


def upload_table(slug: str) -> int:
    path = OUTPUT_ROOT / slug
    if not path.exists():
        raise FileNotFoundError(f"Missing output path: {path}")
    expected = local_rows(path)

    table = bd.Table(dataset_id=DATASET_ID, table_id=slug)

    # Clear the stale staging prefix first, or leftover partitions from an
    # earlier harvest collide with the new partition keys.
    storage = bd.Storage(dataset_id=DATASET_ID, table_id=slug)
    try:
        storage.delete_table(mode="staging", not_found_ok=True)
    except Exception as exc:
        print(f"  [warn] staging prefix cleanup: {exc}")

    table.create(
        path=str(path),
        source_format="parquet",
        if_table_exists="replace",
        if_storage_data_exists="replace",
        if_dataset_exists="pass",
    )

    client = bigquery.Client(project=BILLING_PROJECT)
    query = f"select count(*) as n from `{BILLING_PROJECT}.{DATASET_ID}_staging.{slug}`"
    uploaded = next(iter(client.query(query).result())).n

    if uploaded != expected:
        raise ValueError(
            f"{slug}: uploaded {uploaded:,} != local {expected:,}"
        )
    print(f"  {slug}: uploaded {uploaded:,} rows — OK", flush=True)
    return uploaded


def main() -> None:
    only = set(_argv)
    tables = [t for t in TABLES if not only or t in only]
    print(f"=== uploading to {BILLING_PROJECT} (env={ENV}) ===", flush=True)
    for slug in tables:
        print(f"=== {slug} ===", flush=True)
        try:
            upload_table(slug)
        except Exception as exc:
            print(f"  FAILED: {type(exc).__name__}: {exc}")
            sys.exit(1)
    print("ALL TABLES UPLOADED")


if __name__ == "__main__":
    main()
