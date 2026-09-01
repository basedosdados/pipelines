"""Upload cleaned cl_chilecompra_mercado_publico parquet to BigQuery staging.

    uv run python models/cl_chilecompra_mercado_publico/code/upload.py [--env dev] [table ...]

Point GOOGLE_APPLICATION_CREDENTIALS at the matching service account; this machine's
config.toml is provisioned for dev only. Prod table data is materialised by the
table-approve action when the PR merges, not uploaded from here.

Two deliberate departures from the usual onboarding upload script:

1. It calls the repo's shared ``_upload_to_gcs`` rather than ``bd.Table.create(path=...)``.
   ``create`` reads the whole parquet tree into pandas, which at this dataset's ~150M
   rows is tens of gigabytes of RAM. ``_upload_to_gcs`` builds the staging table from a
   single row group of one file and then streams the rest to GCS.

2. Using the same function as the recurring pipeline keeps both upload paths byte-for-byte
   consistent. They share one staging dataset, so a typed external table left behind by
   one collides with the other's all-STRING overwrite.

Expected row counts come from clean_log.jsonl, written by the cleaning step, so the
check is against what was actually produced rather than a hardcoded number.
"""

from __future__ import annotations

import argparse
import glob
import json
import os
import sys
import warnings
from collections import defaultdict
from pathlib import Path

warnings.filterwarnings("ignore")

import google.cloud.storage as gcs  # noqa: E402
from google.cloud import bigquery  # noqa: E402

from pipelines.utils.tasks import _upload_to_gcs  # noqa: E402

DATASET_ID = "cl_chilecompra_mercado_publico"
TABLES = ["orden_compra_item", "licitacion_item", "licitacion_oferta"]
DEFAULT_ROOT = Path(
    os.environ.get(
        "CHILECOMPRA_DATA_DIR",
        Path.home() / "Downloads" / "cl_chilecompra_mercado_publico_data",
    )
)


def _patch_requester_pays(billing_project: str) -> None:
    """The staging bucket is requester-pays, so every bucket handle needs a user project."""
    original = gcs.Client.bucket

    def patched(self, bucket_name, user_project=None):
        return original(self, bucket_name, user_project=billing_project)

    gcs.Client.bucket = patched


def expected_rows(root: Path) -> dict[str, int]:
    """Sum the per-month row counts the cleaning step recorded, per table."""
    totals: dict[str, int] = defaultdict(int)
    log = root / "clean_log.jsonl"
    if not log.exists():
        return {}
    for line in log.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        for table, count in json.loads(line)["rows"].items():
            totals[table] += count
    return dict(totals)


def partition_count(root: Path, table: str) -> int:
    return len(
        glob.glob(f"{root}/output/{table}/**/data.parquet", recursive=True)
    )


def upload_table(
    table: str, root: Path, billing_project: str, expected: int | None
) -> int:
    path = root / "output" / table
    if not path.exists():
        raise FileNotFoundError(f"missing output path: {path}")
    parts = partition_count(root, table)
    print(f"  {parts} partition file(s) under {path}", flush=True)

    _upload_to_gcs(
        data_path=str(path),
        dataset_id=DATASET_ID,
        table_id=table,
        bucket_name=billing_project,
        dump_mode="append",
        source_format="parquet",
    )

    client = bigquery.Client(project=billing_project)
    query = f"select count(*) as n from `{billing_project}.{DATASET_ID}_staging.{table}`"
    actual = next(iter(client.query(query).result())).n

    if expected is None:
        print(
            f"  {table}: {actual:,} rows in staging (no expected count on record)"
        )
    elif actual == expected:
        print(f"  {table}: {actual:,} rows — matches the cleaning log")
    else:
        raise ValueError(
            f"{table}: staging has {actual:,} rows but the cleaning log recorded "
            f"{expected:,}. Do not continue -- one of the two is wrong."
        )
    return actual


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", default="dev", choices=["dev", "prod"])
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument("tables", nargs="*", default=None)
    args = parser.parse_args()

    billing_project = (
        "basedosdados" if args.env == "prod" else "basedosdados-dev"
    )
    _patch_requester_pays(billing_project)

    wanted = args.tables or TABLES
    totals = expected_rows(args.root)

    print(
        f"=== uploading to {billing_project} (env={args.env}) ===", flush=True
    )
    for table in wanted:
        print(f"=== {table} ===", flush=True)
        try:
            upload_table(table, args.root, billing_project, totals.get(table))
        except Exception as exc:
            print(f"  FAILED: {type(exc).__name__}: {exc}", flush=True)
            print("  Stopping -- do not upload later tables after a failure.")
            return 1
    print("ALL TABLES UPLOADED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
