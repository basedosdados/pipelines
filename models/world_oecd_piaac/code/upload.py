"""Upload the cleaned world_oecd_piaac tables to BigQuery staging.

Usage:
    uv run python models/world_oecd_piaac/code/upload.py [--env dev|prod] [table_slug ...]

Uploads smallest first and stops on the first failure, so a bad table is caught
before the large ones spend time uploading.
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
DATASET_ID = "world_oecd_piaac"
OUTPUT_ROOT = (
    Path(
        os.environ.get(
            "PIAAC_DATA_ROOT",
            Path.home() / "Downloads" / "world_oecd_piaac_data",
        )
    )
    / "output"
)

# The GCS bucket is requester-pays, so every bucket handle needs a billing project.
_orig_bucket = gcs.Client.bucket


def _patched_bucket(self, bucket_name, user_project=None):
    return _orig_bucket(self, bucket_name, user_project=BILLING_PROJECT)


gcs.Client.bucket = _patched_bucket

# (table_slug, expected_rows) -- smallest first. Counts reconcile exactly against
# the source Public Use Files: 227,031 + 157,525 + 3,660 = 388,216 respondents.
TABLES = [
    ("variable", 3_811),
    ("dictionary", 32_299),
    ("respondent_cycle_1_usa_national", 3_660),
    ("respondent_cycle_2", 157_525),
    ("respondent_cycle_1", 227_031),
    ("item_response_cycle_2", 10_927_570),
    ("item_response_cycle_1", 13_905_983),
]


def upload_table(slug: str, expected_rows: int) -> int:
    path = OUTPUT_ROOT / slug
    if not path.exists():
        raise FileNotFoundError(f"Missing output path: {path}")

    storage = bd.Storage(dataset_id=DATASET_ID, table_id=slug)
    try:
        storage.delete_table(mode="staging", not_found_ok=True)
    except Exception as exc:
        print(f"  [warn] staging prefix cleanup: {exc}")

    bd.Table(dataset_id=DATASET_ID, table_id=slug).create(
        path=str(path),
        source_format="parquet",
        if_table_exists="replace",
        if_storage_data_exists="replace",
        if_dataset_exists="pass",
    )

    client = bigquery.Client(project=BILLING_PROJECT)
    query = f"select count(*) as n from `{BILLING_PROJECT}.{DATASET_ID}_staging.{slug}`"
    rows = next(iter(client.query(query).result())).n
    status = "OK" if rows == expected_rows else "ROW MISMATCH"
    print(
        f"  {slug}: uploaded {rows:,} rows (expected {expected_rows:,}) — {status}"
    )
    if rows != expected_rows:
        raise ValueError(
            f"{slug}: row count {rows:,} != expected {expected_rows:,}"
        )
    return rows


def main() -> None:
    only = set(_argv)
    tables = [(s, r) for s, r in TABLES if not only or s in only]
    print(f"=== uploading to {BILLING_PROJECT} (env={ENV}) ===", flush=True)
    for slug, expected in tables:
        print(f"=== {slug} ===", flush=True)
        try:
            upload_table(slug, expected)
        except Exception as exc:
            print(f"  FAILED: {type(exc).__name__}: {exc}")
            sys.exit(1)
    print("ALL TABLES UPLOADED")


if __name__ == "__main__":
    main()
