"""Upload the cleaned parquet to BigQuery staging in basedosdados-dev.

    uv run python upload.py                # every table
    uv run python upload.py general        # one table

Each table's BigQuery row count is checked against the local parquet row count
and the run stops at the first mismatch, so a partial upload never passes for
a complete one.

Requires GOOGLE_APPLICATION_CREDENTIALS pointing at a dev service-account key
and ~/.basedosdados/config.toml. The bucket is requester-pays, so
gcs.Client.bucket is pinned to the billing project.
"""

import sys

import basedosdados as bd
import constants as c
import google.cloud.storage as gcs
import layout
import pyarrow as pa
import pyarrow.parquet as pq

BILLING_PROJECT = "basedosdados-dev"

_original_bucket = gcs.Client.bucket


def _bucket(self, bucket_name, user_project=None):
    return _original_bucket(self, bucket_name, user_project=BILLING_PROJECT)


gcs.Client.bucket = _bucket


def write_header_stub(table: str) -> None:
    """Seed a 0-row parquet that sorts first in the staging prefix.

    The table-approve action infers a table's schema by reading the first blob
    in the prefix with pandas, loading the whole file to learn only its column
    names. On a multi-million-row partition that exhausts the CI runner and
    prod materialisation dies with no traceback. A 0-row file named so it
    sorts ahead of "year=..." is read instead, and contributes no rows.
    """
    if table in layout.UNPARTITIONED:
        return
    root = c.OUTPUT_DIR / table
    stub = root / "00_header.parquet"
    # Built from the layout rather than copied from a partition: the schema is
    # all-STRING by definition, so the stub cannot inherit a partition's type.
    schema = pa.schema(
        [pa.field(name, pa.string()) for name in layout.LAYOUT[table]]
    )
    pq.write_table(
        pa.Table.from_pylist([], schema=schema), stub, compression="snappy"
    )


def local_rows(table: str) -> tuple[int, int]:
    files = sorted((c.OUTPUT_DIR / table).rglob("*.parquet"))
    return sum(pq.ParquetFile(f).metadata.num_rows for f in files), len(files)


def check_partitions(table: str) -> None:
    """Reject an upload whose program years are not exactly the expected set.

    Counting files is not enough: a missing program year alongside a stale one
    gives the right total and the wrong data, and staging is dropped before
    the upload, so the mistake is not recoverable from the previous state.
    """
    root = c.OUTPUT_DIR / table
    partitions = [
        f for f in root.rglob("*.parquet") if f.name != "00_header.parquet"
    ]
    if table in layout.UNPARTITIONED:
        if len(partitions) != 1:
            raise ValueError(f"{table}: {len(partitions)} file(s), expected 1")
        return
    found = {int(f.parent.name.split("=")[1]) for f in partitions}
    want = set(layout.COVERAGE[table])
    if found != want:
        raise ValueError(
            f"{table}: partitions missing={sorted(want - found)} "
            f"unexpected={sorted(found - want)}. Finish run_all.py before uploading."
        )


def upload_table(table: str) -> int:
    path = c.OUTPUT_DIR / table
    check_partitions(table)
    write_header_stub(table)
    rows, files = local_rows(table)
    print(f"[{table}] local: {rows:,} rows across {files} parquet file(s)")

    storage = bd.Storage(dataset_id=c.GCP_DATASET_ID, table_id=table)
    storage.delete_table(mode="staging", not_found_ok=True)

    bd.Table(dataset_id=c.GCP_DATASET_ID, table_id=table).create(
        path=str(path),
        source_format="parquet",
        if_table_exists="replace",
        if_storage_data_exists="replace",
        if_dataset_exists="pass",
    )

    query = f"select count(*) as n from `{BILLING_PROJECT}.{c.GCP_DATASET_ID}_staging.{table}`"
    got = int(
        bd.read_sql(query, billing_project_id=BILLING_PROJECT, from_file=True)[
            "n"
        ].iloc[0]
    )
    verdict = "MATCH" if got == rows else "MISMATCH"
    print(f"[{table}] uploaded - bigquery={got:,} local={rows:,} {verdict}")
    if got != rows:
        # Leave nothing behind that a later materialisation could consume:
        # the staging table failed validation, so it should not exist.
        storage.delete_table(mode="staging", not_found_ok=True)
        raise ValueError(f"row count mismatch for {table}")
    return got


if __name__ == "__main__":
    targets = sys.argv[1:] or list(layout.LAYOUT)
    total = 0
    for table in targets:
        total += upload_table(table)
    print(f"\n{len(targets)} table(s) uploaded, {total:,} rows")
