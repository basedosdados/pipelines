"""Upload cleaned br_mf_divida_ativa parquet tables to BigQuery.

Reads partitioned parquet from the data root (outside Dropbox; override with
PGFN_DATA_ROOT) and loads each into the dataset's staging. Row count is verified
against the local parquet footers (metadata-only, so cheap even for ~1B rows).

Usage:
    uv run python models/br_mf_divida_ativa/code/upload.py [--env dev|prod] [table ...]

--env dev (default) -> basedosdados-dev; --env prod -> basedosdados. Point
GOOGLE_APPLICATION_CREDENTIALS at the matching service account. Smallest table
first; stops on first failure.
"""

import argparse
import os
import sys
import warnings
from pathlib import Path

warnings.filterwarnings("ignore")

import basedosdados as bd  # noqa: E402
import google.cloud.storage as gcs  # noqa: E402
import pyarrow.dataset as pads  # noqa: E402
from google.cloud import bigquery  # noqa: E402

DATASET_ID = "br_mf_divida_ativa"
# smallest first, so a bad run fails fast on a cheap table
TABLES = ["fgts", "previdenciario", "nao_previdenciario"]


def data_root() -> Path:
    """Root holding cleaned parquet under ``output/`` (override PGFN_DATA_ROOT)."""
    return Path(
        os.environ.get(
            "PGFN_DATA_ROOT",
            str(Path.home() / "Downloads" / "br_mf_divida_ativa_data"),
        )
    )


def install_requester_pays_patch(billing_project: str) -> None:
    """Route every GCS bucket call through ``billing_project`` (requester-pays)."""
    orig_bucket = gcs.Client.bucket

    def patched(self, bucket_name, user_project=None):
        return orig_bucket(self, bucket_name, user_project=billing_project)

    gcs.Client.bucket = patched


def upload_table(slug: str, billing_project: str, output_root: Path) -> int:
    """Create the staging table for one table slug and verify its row count.

    Args:
        slug: Table slug (also the output subdirectory name).
        billing_project: GCP project billed for the load and verify query.
        output_root: Root directory holding ``<slug>/`` partitioned parquet.

    Returns:
        The local parquet row count (authoritative).

    Raises:
        FileNotFoundError: If the table's output directory is missing.
        ValueError: If the BigQuery row count disagrees with the local parquet.
    """
    path = output_root / slug
    if not path.exists():
        raise FileNotFoundError(f"Missing output path: {path}")

    expected = pads.dataset(path, format="parquet").count_rows()

    tb = bd.Table(dataset_id=DATASET_ID, table_id=slug)
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

    # Row-count cross-check via a BQ query. Best-effort: the staging external
    # table is created over the exact parquet files, so the local count is
    # authoritative. On dev the QueryUsagePerDay quota can block this query -
    # warn and continue rather than fail the whole upload.
    try:
        client = bigquery.Client(project=billing_project)
        q = (
            f"select count(*) as n from "
            f"`{billing_project}.{DATASET_ID}_staging.{slug}`"
        )
        n = next(iter(client.query(q).result())).n
        status = "OK" if n == expected else "ROW MISMATCH"
        print(
            f"  {slug}: uploaded {n:,} rows (local parquet {expected:,}) - {status}"
        )
        if n != expected:
            raise ValueError(f"{slug}: BQ {n:,} != local {expected:,}")
    except ValueError:
        raise
    except Exception as e:
        print(
            f"  {slug}: uploaded ~{expected:,} rows (local parquet); "
            f"BQ verify skipped - {type(e).__name__}: {str(e)[:120]}"
        )
    return expected


def main() -> None:
    """Parse args and upload the selected tables (smallest first) to staging."""
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--env", choices=["dev", "prod"], default="dev")
    ap.add_argument(
        "tables",
        nargs="*",
        choices=TABLES,
        help="tables to upload (default: all, smallest first)",
    )
    args = ap.parse_args()

    billing_project = (
        "basedosdados" if args.env == "prod" else "basedosdados-dev"
    )
    install_requester_pays_patch(billing_project)
    output_root = data_root() / "output"

    selected = set(args.tables)
    tables = [t for t in TABLES if not selected or t in selected]

    print(
        f"=== uploading to {billing_project} (env={args.env}) ===", flush=True
    )
    for slug in tables:
        print(f"=== {slug} ===", flush=True)
        try:
            upload_table(slug, billing_project, output_root)
        except Exception as e:
            print(f"  FAILED: {type(e).__name__}: {e}")
            sys.exit(1)
    print("ALL TABLES UPLOADED")


if __name__ == "__main__":
    main()
