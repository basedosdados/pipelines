#!/usr/bin/env python3
"""Upload the cleaned ar_indec_eph parquet to the dev staging bucket.

Goes through `_upload_to_gcs` -- the helper the recurring flow's task wraps --
rather than `bd.Table.create(path=<data>)` or a BigQuery load job, for the two
reasons cl_ine_ene's uploader documents:

* `bd.Table.create` given the full parquet reads all of it into pandas and
  stringifies it. Through this helper it only ever sees a one-row header file, so
  memory stays flat while `Storage.upload` streams the partitions.
* A `load_table_from_uri` job would leave staging as a NATIVE table, which never
  re-reads the GCS files. dbt would then keep serving this bootstrap snapshot
  after every later release, silently. This helper leaves an EXTERNAL table over
  gs://<bucket>/staging/<ds>/<table>/, so the onboarding and the pipeline agree.

    uv run python models/ar_indec_eph/code/upload.py --check
    uv run python models/ar_indec_eph/code/upload.py --table microdatos_hogar

Needs GOOGLE_APPLICATION_CREDENTIALS pointing at the dev service-account key:

    GOOGLE_APPLICATION_CREDENTIALS=~/.basedosdados/credentials/staging.json \
        uv run python models/ar_indec_eph/code/upload.py
"""

from __future__ import annotations

import argparse
import pathlib
import sys

REPO = pathlib.Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

from constants import OUTPUT_DIR, TABLES  # noqa: E402

DATASET_ID = "ar_indec_eph"
BUCKET = "basedosdados-dev"
ALL_TABLES = [*TABLES, "dicionario"]


def partition_files(table: str) -> list[pathlib.Path]:
    root = OUTPUT_DIR / table
    if table == "dicionario":
        return sorted(root.glob("data.parquet"))
    return sorted(root.glob("ano=*/trimestre=*/data.parquet"))


def verify_local(table: str, expected_partitions: int | None = None) -> int:
    """Refuse to upload a partition set that is not uniform and complete."""
    import pyarrow.parquet as pq

    files = partition_files(table)
    if not files:
        raise SystemExit(
            f"no parquet for {table} under {OUTPUT_DIR} -- run eph_clean.py"
        )

    schemas, rows = set(), 0
    for parquet in files:
        handle = pq.ParquetFile(parquet)
        schema = handle.schema_arrow
        schemas.add((tuple(schema.names), tuple(str(t) for t in schema.types)))
        rows += handle.metadata.num_rows
    if len(schemas) != 1:
        raise SystemExit(
            f"{table}: {len(schemas)} distinct schemas across partitions; expected 1"
        )
    names, types = next(iter(schemas))
    if set(types) != {"string"}:
        raise SystemExit(
            f"{table}: staging parquet must be all-STRING, found {sorted(set(types))}"
        )
    if expected_partitions is not None and len(files) != expected_partitions:
        raise SystemExit(
            f"{table}: {len(files)} partitions, expected {expected_partitions}"
        )
    print(
        f"{table}: {len(files)} partitions, {rows:,} rows, {len(names)} all-STRING columns"
    )
    return rows


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--check", action="store_true", help="verify locally, upload nothing"
    )
    parser.add_argument(
        "--table",
        action="append",
        choices=ALL_TABLES,
        help="upload only this table (repeatable); default all",
    )
    parser.add_argument(
        "--dump-mode", default="overwrite", choices=["overwrite", "append"]
    )
    parser.add_argument(
        "--expect-partitions",
        type=int,
        default=None,
        help="fail unless each microdata table has exactly this many partitions",
    )
    args = parser.parse_args()

    tables = args.table or ALL_TABLES
    for table in tables:
        verify_local(
            table, None if table == "dicionario" else args.expect_partitions
        )
    if args.check:
        return 0

    from pipelines.utils.tasks import _upload_to_gcs

    for table in tables:
        data_path = OUTPUT_DIR / table
        print(
            f"\nuploading {table} -> gs://{BUCKET}/staging/{DATASET_ID}/{table}/ "
            f"(dump_mode={args.dump_mode})"
        )
        _upload_to_gcs(
            data_path=data_path,
            dataset_id=DATASET_ID,
            table_id=table,
            bucket_name=BUCKET,
            dump_mode=args.dump_mode,
            source_format="parquet",
        )
        print(f"{table}: done")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
