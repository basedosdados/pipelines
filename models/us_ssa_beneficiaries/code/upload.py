"""Upload cleaned us_ssa_beneficiaries parquet tables to BigQuery (one-shot onboarding).

Usage:
    uv run python models/us_ssa_beneficiaries/code/upload.py [--env dev|prod] [table ...]

Dev only: prod tables are materialised by the table-approve action when the
onboarding PR merges, never uploaded from here. Staging is
ALL-STRING (the parquet is all-STRING; the dbt model safe_casts). Uploads
smallest first and stops on the first failure.

Scratch parquet lives under ``~/Downloads/us_ssa_beneficiaries_data/output`` (override
with SSA_DATA_DIR).
"""

import os
import sys
import warnings
from pathlib import Path

warnings.filterwarnings("ignore")

import basedosdados as bd  # noqa: E402
import google.cloud.storage as gcs  # noqa: E402
import pyarrow.dataset as pads  # noqa: E402
from google.cloud import bigquery  # noqa: E402

_argv = sys.argv[1:]
if "--env" in _argv:
    _i = _argv.index("--env")
    if _i + 1 >= len(_argv):
        raise SystemExit("--env requires a value: dev or prod")
    ENV = _argv[_i + 1]
    _argv = _argv[:_i] + _argv[_i + 2 :]
else:
    ENV = "dev"
if ENV != "dev":
    # Only dev. bd.Table and bd.Storage take no project argument -- they read
    # ~/.basedosdados/config.toml, which is provisioned for basedosdados-dev --
    # so `--env prod` would bill prod for the GCS write while the BigQuery
    # table still landed in dev. That mismatch is silent.
    #
    # Prod data is not uploaded from a laptop at all. It is materialised by the
    # table-approve action when the onboarding PR merges, which runs
    # `dbt --target prod`. See .claude/rules/onboarding-workflow.md.
    raise SystemExit(
        f"--env must be dev, got {ENV!r}. Prod tables are materialised by "
        f"table-approve on merge, never uploaded locally."
    )
BILLING_PROJECT = "basedosdados-dev"
# The upload itself is billed to the data project, but a local ADC user has no
# bigquery.jobs.create there, so the read-back verification is billed
# separately.  Override with BD_QUERY_BILLING_PROJECT.
QUERY_BILLING_PROJECT = os.environ.get(
    "BD_QUERY_BILLING_PROJECT", BILLING_PROJECT
)
DATASET_ID = "us_ssa_beneficiaries"
OUTPUT_ROOT = (
    Path(
        os.environ.get(
            "SSA_DATA_DIR",
            os.path.expanduser("~/Downloads/us_ssa_beneficiaries_data"),
        )
    )
    / "output"
)

# Monkey-patch for requester-pays bucket.
_orig_bucket = gcs.Client.bucket


def _patched_bucket(
    self: gcs.Client, bucket_name: str, user_project: str | None = None
) -> "gcs.Bucket":
    """Return a bucket handle billed to ``BILLING_PROJECT`` (requester-pays).

    Args:
        self: The storage client (this replaces ``gcs.Client.bucket``).
        bucket_name: The bucket to open.
        user_project: Ignored; the billing project is forced to
            ``BILLING_PROJECT``.

    Returns:
        The bucket handle with ``user_project`` set for requester-pays access.
    """
    return _orig_bucket(self, bucket_name, user_project=BILLING_PROJECT)


gcs.Client.bucket = _patched_bucket

# Per-grain fact tables + dicionario. Expected row counts are read from the
# cleaned parquet at runtime (no hardcoding), then verified against BigQuery.
TABLES = [
    "dicionario",
    "oasdi_population_share",
    "ssi_state",
    "oasdi_state",
    "ssi_county",
    "oasdi_county",
]


def parquet_rows(path: Path) -> int:
    """Count rows across a table's cleaned parquet partitions.

    Args:
        path: The table's output directory (holds ``year=*/data.parquet``).

    Returns:
        The total row count.
    """
    # Glob files (not the dir) so pyarrow reads `year` from the file column
    # rather than inferring it from the hive path (string-vs-int conflict).
    files = [str(p) for p in path.rglob("*.parquet")]
    return pads.dataset(files, format="parquet").count_rows()


def upload_table(slug: str) -> int:
    """Upload one cleaned table to BigQuery staging and verify its row count.

    Args:
        slug: Table slug (a key of :data:`TABLES`).

    Returns:
        The row count read back from BigQuery.

    Raises:
        FileNotFoundError: If the table's output directory is missing.
        ValueError: If the uploaded row count differs from the parquet count.
    """
    path = OUTPUT_ROOT / slug
    if not path.exists():
        raise FileNotFoundError(f"Missing output path: {path}")
    expected_rows = parquet_rows(path)

    tb = bd.Table(dataset_id=DATASET_ID, table_id=slug)

    # Delete stale GCS staging prefix (avoids BQ partition key conflicts).
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

    client = bigquery.Client(project=QUERY_BILLING_PROJECT)
    q = f"select count(*) as n from `{BILLING_PROJECT}.{DATASET_ID}_staging.{slug}`"
    n = next(iter(client.query(q).result())).n
    status = "OK" if n == expected_rows else "ROW MISMATCH"
    print(
        f"  {slug}: uploaded {n:,} rows (expected {expected_rows:,}) — {status}"
    )
    if n != expected_rows:
        raise ValueError(
            f"{slug}: row count {n:,} != expected {expected_rows:,}"
        )
    return n


def main() -> None:
    """Upload the selected tables (or all of them) to BigQuery, smallest first.

    Reads table slugs from the command line (after ``--env``); with none given,
    uploads every table in :data:`TABLES`. Exits non-zero on the first failure.

    Raises:
        ValueError: If any positional argument is not a known table slug.
    """
    only = set(_argv)
    unknown = only.difference(TABLES)
    if unknown:
        raise ValueError(
            f"unknown table(s): {sorted(unknown)}; valid: {TABLES}"
        )
    tables = [s for s in TABLES if not only or s in only]
    print(f"=== uploading to {BILLING_PROJECT} (env={ENV}) ===", flush=True)
    for slug in tables:
        print(f"=== {slug} ===", flush=True)
        try:
            upload_table(slug)
        except Exception as e:
            print(f"  FAILED: {type(e).__name__}: {e}")
            sys.exit(1)
    print("ALL TABLES UPLOADED")


if __name__ == "__main__":
    main()
