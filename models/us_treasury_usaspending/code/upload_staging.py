"""Upload the cleaned parquet to the BigQuery dev staging dataset.

Deliberately does not use ``bd.Table.create``: that reads the first parquet
file into pandas and calls ``astype(str)`` on it, which needs tens of GB of RAM
for files this size. Instead the objects go to GCS directly and the external
table is defined through the BigQuery API, with hive partitioning on
``fiscal_year`` — the same shape ``upload_to_gcs`` produces for the recurring
pipeline, so the two paths cannot diverge.

A zero-row ``00_header.parquet`` is written into the first partition of every
table. The table-approve CI job reads the *first* parquet of a staging table to
build its header, whole, into pandas; a multi-hundred-MB first file OOMs the
runner. Sorting a 0-row file ahead of the real data avoids that.

Usage:
    GOOGLE_APPLICATION_CREDENTIALS=~/.basedosdados/credentials/staging.json \
      uv run python models/us_treasury_usaspending/code/upload_staging.py
    ... --tables contract_transaction --dry-run
"""

from __future__ import annotations

import argparse
import os
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import bigquery, storage

from pipelines.datasets.us_treasury_usaspending.constants import constants
from pipelines.datasets.us_treasury_usaspending.utils import staging_schema

DATASET_ID = constants.DATASET_ID.value
PARTITION = constants.PARTITION_COLUMN.value
DATA_DIR = Path(
    os.environ.get(
        "USASPENDING_DATA_DIR",
        Path.home() / "Downloads" / "us_treasury_usaspending_data",
    )
)
OUTPUT = DATA_DIR / "output"
HEADER_NAME = "00_header.parquet"
CHUNK = 256 * 1024 * 1024


def write_header(table: str, partition_dir: Path) -> Path:
    """Zero-row parquet carrying the table's staging schema."""
    path = partition_dir / HEADER_NAME
    schema = staging_schema(table)
    empty = pa.Table.from_arrays(
        [pa.array([], type=pa.string()) for _ in schema.names], schema=schema
    )
    pq.write_table(empty, path, compression="snappy")
    return path


def _upload_with_retry(blob, path: Path, attempts: int = 6) -> None:
    """Upload one object, retrying on transport errors.

    These objects run to a gigabyte each and the uplink is usually shared with
    the archive download, so a write timeout mid-transfer is routine rather than
    exceptional. A resumable upload restarts from the beginning, so the retry is
    simply reissued.
    """
    delay = 15
    for attempt in range(1, attempts + 1):
        try:
            blob.upload_from_filename(str(path), timeout=3600)
            return
        except Exception as e:  # noqa: BLE001
            if attempt == attempts:
                raise
            print(
                f"    retry {attempt} for {path.name}: {type(e).__name__}",
                flush=True,
            )
            time.sleep(delay)
            delay = min(delay * 2, 300)


def upload_table(
    table: str,
    project: str,
    bucket_name: str,
    dry_run: bool,
    skip_existing: bool = False,
) -> tuple[int, int]:
    table_dir = OUTPUT / table
    if not table_dir.exists():
        raise SystemExit(f"missing cleaned output for {table}: {table_dir}")

    partitions = sorted(p for p in table_dir.iterdir() if p.is_dir())
    if partitions:
        header = write_header(table, partitions[0])
        print(f"  header: {header.relative_to(OUTPUT)}")
    files = sorted(table_dir.rglob("*.parquet"))
    total = sum(f.stat().st_size for f in files)
    print(f"  {len(files)} files, {total / 1e9:.1f} GB")
    if dry_run:
        return len(files), total

    client = storage.Client(project=project)
    bucket = client.bucket(bucket_name, user_project=project)
    skipped = 0
    for f in files:
        rel = f.relative_to(table_dir)
        blob = bucket.blob(f"staging/{DATASET_ID}/{table}/{rel.as_posix()}")
        if skip_existing:
            # Cleaning writes one deterministically-named object per fiscal
            # year, so a same-size blob is the same content and re-sending it
            # would only cost time. Lets the upload run alongside cleaning.
            existing = bucket.get_blob(blob.name)
            if existing and existing.size == f.stat().st_size:
                skipped += 1
                continue
        blob.chunk_size = CHUNK
        _upload_with_retry(blob, f)
        print(
            f"    uploaded {rel.as_posix()} ({f.stat().st_size / 1e6:.0f} MB)",
            flush=True,
        )
    if skipped:
        print(f"    {skipped} object(s) already present, skipped")

    bq = bigquery.Client(project=project)
    bq.create_dataset(f"{project}.{DATASET_ID}_staging", exists_ok=True)
    prefix = f"gs://{bucket_name}/staging/{DATASET_ID}/{table}/"
    config = bigquery.ExternalConfig("PARQUET")
    config.source_uris = [f"{prefix}*"]
    config.hive_partitioning = bigquery.HivePartitioningOptions()
    config.hive_partitioning.mode = "STRINGS"
    config.hive_partitioning.source_uri_prefix = prefix
    bq_table = bigquery.Table(f"{project}.{DATASET_ID}_staging.{table}")
    bq_table.external_data_configuration = config
    bq.delete_table(bq_table, not_found_ok=True)
    bq.create_table(bq_table)
    print(f"  external table {DATASET_ID}_staging.{table} created")

    n = next(
        bq.query(
            f"select count(*) c from `{project}.{DATASET_ID}_staging.{table}`"
        ).result()
    ).c
    print(f"  staging row count: {n:,}")
    return len(files), total


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--project", default="basedosdados-dev")
    ap.add_argument("--bucket", default="basedosdados-dev")
    ap.add_argument("--tables", nargs="*", default=constants.TABLES.value)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument(
        "--skip-existing",
        action="store_true",
        help="do not re-send objects already in GCS at the same size",
    )
    args = ap.parse_args()

    for table in args.tables:
        print(f"{table}:")
        upload_table(
            table,
            args.project,
            args.bucket,
            args.dry_run,
            skip_existing=args.skip_existing,
        )


if __name__ == "__main__":
    main()
