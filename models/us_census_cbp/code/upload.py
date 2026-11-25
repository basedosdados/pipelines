"""Upload cleaned parquet tables of us_census_cbp to BigQuery staging (basedosdados-dev).

national/state/county are hive-partitioned by year (year=<YYYY>/data.parquet);
dicionario is a single file. Verifies each staging table's row count against the
local parquet row count. Stops at the first failure.

Requires GOOGLE_APPLICATION_CREDENTIALS pointing at a BD service-account key with
write access to basedosdados-dev, plus ~/.basedosdados/config.toml.
"""

import glob
import sys
from pathlib import Path

import basedosdados as bd
import google.cloud.storage as gcs
import pyarrow.parquet as pq

BILLING_PROJECT = "basedosdados-dev"
DATASET_ID = "us_census_cbp"
ROOT = Path(__file__).resolve().parent.parent
OUTPUT_DIR = ROOT / "data" / "output"
TABLES = ["national", "state", "county", "dicionario"]
# national/state/county carry one parquet per reference year; dicionario is a single
# unpartitioned file. Checked before the staging table is dropped, so a partial local
# export cannot silently replace a complete staging table with fewer years.
YEARS = list(range(1998, 2024))
EXPECTED_FILES = {
    "national": len(YEARS),
    "state": len(YEARS),
    "county": len(YEARS),
    "dicionario": 1,
}

_orig_bucket = gcs.Client.bucket


def _patched_bucket(self, bucket_name, user_project=None):
    return _orig_bucket(self, bucket_name, user_project=BILLING_PROJECT)


gcs.Client.bucket = _patched_bucket


def local_rows(table_slug: str) -> tuple[int, int]:
    """Count rows and files in the local parquet export for a table.

    Args:
        table_slug: Table slug under ``data/output/``.

    Returns:
        A ``(row_count, file_count)`` pair read from the parquet footers.
    """
    files = glob.glob(
        str(OUTPUT_DIR / table_slug / "**" / "*.parquet"), recursive=True
    )
    return sum(pq.ParquetFile(f).metadata.num_rows for f in files), len(files)


def check_complete(table_slug: str, nfiles: int) -> None:
    """Fail before touching staging if the local export is incomplete.

    ``local_rows`` counts whatever happens to be on disk, so an interrupted clean
    run would otherwise upload a short table and still report a row-count match.

    Args:
        table_slug: Table slug under ``data/output/``.
        nfiles: Number of parquet files found locally.

    Raises:
        ValueError: If the file count differs from the expected partition count.
    """
    want = EXPECTED_FILES[table_slug]
    if nfiles != want:
        raise ValueError(
            f"Incomplete local export for {table_slug}: found {nfiles} parquet "
            f"file(s), expected {want}. Re-run clean.py before uploading."
        )


def upload_table(table_slug: str) -> int:
    """Replace a staging table with the local parquet export and verify it.

    Args:
        table_slug: Table slug under ``data/output/``.

    Returns:
        The row count reported by BigQuery after upload.

    Raises:
        ValueError: If the export is incomplete or the row counts disagree.
    """
    path = OUTPUT_DIR / table_slug
    expected, nfiles = local_rows(table_slug)
    print(
        f"[{table_slug}] local: {expected:,} rows across {nfiles} parquet file(s)"
    )
    check_complete(table_slug, nfiles)

    st = bd.Storage(dataset_id=DATASET_ID, table_id=table_slug)
    st.delete_table(mode="staging", not_found_ok=True)

    tb = bd.Table(dataset_id=DATASET_ID, table_id=table_slug)
    tb.create(
        path=str(path),
        source_format="parquet",
        if_table_exists="replace",
        if_storage_data_exists="replace",
        if_dataset_exists="pass",
    )

    query = (
        f"select count(*) as n from "
        f"`{BILLING_PROJECT}.{DATASET_ID}_staging.{table_slug}`"
    )
    df = bd.read_sql(query, billing_project_id=BILLING_PROJECT, from_file=True)
    n = int(df["n"].iloc[0])
    ok = "MATCH" if n == expected else "MISMATCH"
    print(f"[{table_slug}] uploaded - bq={n:,} expected={expected:,} {ok}")
    if n != expected:
        raise ValueError(
            f"Row-count mismatch for {table_slug}: bq={n}, local={expected}"
        )
    return n


def main() -> None:
    """Upload the tables named on the command line, or all of them by default."""
    only = sys.argv[1:] or TABLES
    unknown = [t for t in only if t not in TABLES]
    if unknown:
        print(
            f"Unknown table(s): {', '.join(unknown)}. "
            f"Valid tables: {', '.join(TABLES)}."
        )
        sys.exit(2)
    for table_slug in TABLES:
        if table_slug not in only:
            continue
        try:
            upload_table(table_slug)
        except Exception as e:
            print(f"[{table_slug}] FAILED - {e}")
            sys.exit(1)
    print(f"CBP uploads complete: {', '.join(only)}.")


if __name__ == "__main__":
    main()
