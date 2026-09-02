#!/usr/bin/env python3
"""
Upload the cleaned College Scorecard parquet to BigQuery dev.

Streams each partition to GCS and then issues one server-side load job per
table, rather than calling ``bd.Table.create``. ``Table.create`` reads the
whole parquet into pandas and stringifies it before dumping to GCS, which on
a 135.8M-row table balloons RSS into the tens of GB; streaming keeps memory
flat regardless of table size. See [[reference_bd_table_create_ram_blowup]].

The GCS layout is the same one ``bd.Storage`` would write --
``staging/<gcp_dataset_id>/<table>/year=YYYY/data.parquet`` -- so the
table-approve job finds the files at merge time. Keeping the hive directories
costs nothing here because ``year`` is also a real column inside every file
(house convention, cf. us_bls_cpi.write_partitioned), so the load job reads it
from the data and never needs the directory name.

Row counts are verified in BigQuery against clean_stats.json after each load,
and the run stops on the first mismatch rather than continuing with a
half-loaded dataset.

Usage:
    GOOGLE_APPLICATION_CREDENTIALS=~/.basedosdados/credentials/staging.json \
        uv run python models/us_ed_college_scorecard/code/upload.py [table ...]
"""

import json
import os
import pathlib
import sys
import warnings

warnings.filterwarnings("ignore")

import pyarrow as pa  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402
from google.cloud import bigquery, storage  # noqa: E402

PROJECT = "basedosdados-dev"
BUCKET = "basedosdados-dev"
DATASET_ID = "us_ed_college_scorecard"
CHUNK_SIZE = 256 * 1024 * 1024

CODE_DIR = pathlib.Path(__file__).resolve().parent
OUTPUT_ROOT = pathlib.Path(
    os.environ.get(
        "OUTPUT_DIR",
        pathlib.Path.home() / "Downloads/us_ed_college_scorecard_data/output",
    )
)


def write_zero_row_header(table_dir):
    """Prepend a 0-row parquet so table-approve reads a tiny first file.

    The prod table-approve job builds its schema from the first parquet under
    the staging prefix and loads that file into memory; on a multi-hundred-MB
    first partition it OOMs the runner. A 0-row file sorting before every
    ``year=`` directory fixes it and costs nothing.
    See [[project_table_approve_parquet_header_oom]].
    """
    partitions = sorted(table_dir.glob("year=*/data.parquet"))
    if not partitions:
        return
    header = table_dir / "00_header.parquet"
    schema = pq.read_schema(partitions[0])
    pq.write_table(
        pa.Table.from_pylist([], schema=schema), header, compression="snappy"
    )


def local_files(table):
    root = OUTPUT_ROOT / table
    write_zero_row_header(root)
    return sorted(root.glob("year=*/data.parquet")) or sorted(
        root.glob("*.parquet")
    )


def upload_to_gcs(bucket, table, files):
    """Stream each partition to GCS; returns the gs:// URIs that were written."""
    root = OUTPUT_ROOT / table
    uris = []
    for path in files:
        key = f"staging/{DATASET_ID}/{table}/{path.relative_to(root)}"
        blob = bucket.blob(key)
        blob.chunk_size = CHUNK_SIZE
        blob.upload_from_filename(str(path))
        uris.append(f"gs://{BUCKET}/{key}")
    return uris


def load_table(bq, table, uris):
    """Load the staged parquet into a native staging table.

    A load job cannot overwrite an EXTERNAL table, which is what
    ``bd.Table.create`` leaves behind, so any existing table is dropped first.
    dbt reads either kind, and table-approve rebuilds prod staging itself.
    """
    target = f"{PROJECT}.{DATASET_ID}_staging.{table}"
    bq.query(f"drop table if exists `{target}`").result()
    job = bq.load_table_from_uri(
        uris,
        target,
        job_config=bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        ),
    )
    job.result()
    return next(
        iter(bq.query(f"select count(*) as n from `{target}`").result())
    ).n


def main():
    expected = json.loads((CODE_DIR / "clean_stats.json").read_text())["rows"]
    only = set(sys.argv[1:])
    tables = [
        t
        for t, _ in sorted(expected.items(), key=lambda kv: kv[1])
        if not only or t in only
    ]

    gcs = storage.Client(project=PROJECT)
    bucket = gcs.bucket(BUCKET, user_project=PROJECT)  # requester-pays
    bq = bigquery.Client(project=PROJECT)

    for table in tables:
        files = local_files(table)
        print(
            f"=== {table}: {len(files)} files, {expected[table]:,} rows expected",
            flush=True,
        )
        uris = upload_to_gcs(bucket, table, files)
        loaded = load_table(bq, table, uris)
        ok = loaded == expected[table]
        print(
            f"    loaded {loaded:,} rows — {'OK' if ok else 'ROW MISMATCH'}",
            flush=True,
        )
        if not ok:
            print(f"    FAILED: {loaded:,} != {expected[table]:,}")
            sys.exit(1)
    print("ALL TABLES UPLOADED")


if __name__ == "__main__":
    main()
