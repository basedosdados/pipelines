"""Upload the cleaned us_eia_consumption parquet to BigQuery staging (basedosdados-dev).

    python upload.py                       # every table
    python upload.py retail_sales eia861m  # selected tables

Uses ``pipelines.utils.tasks._upload_to_gcs`` — the same helper the recurring
flow calls — so staging is left EXTERNAL over the GCS prefix (a native load-job
table would silently ignore files a later pipeline run writes).
``dump_mode="append"`` because ``"overwrite"`` drops the production table too.

Requires GOOGLE_APPLICATION_CREDENTIALS (dev service-account key) and
~/.basedosdados/config.toml. The bucket is requester-pays, so
``gcs.Client.bucket`` is patched to pin ``user_project``.
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

# Minimum expected partition counts (year sets differ per table): utility and
# retail_sales run 2001-2025, service_territory 2012-2025, eia861m 1990-present.
MIN_FILES = {
    "utility": 24,
    "retail_sales": 24,
    "service_territory": 13,
    "eia861m": 35,
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
    if nfiles < MIN_FILES[table]:
        raise SystemExit(
            f"[{table}] expected at least {MIN_FILES[table]} parquet file(s), found "
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
            f"[{table}] staging table is {ref.table_type}, expected EXTERNAL"
        )
    if got != expected:
        raise SystemExit(
            f"[{table}] ROW COUNT MISMATCH: {got:,} != {expected:,}"
        )
    typed = [f.name for f in ref.schema if f.field_type != "STRING"]
    if typed:
        raise SystemExit(
            f"[{table}] staging schema is not all-STRING: {typed}"
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
