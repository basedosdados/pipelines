"""Upload the climatologiques tables to the ``basedosdados-dev`` staging dataset.

    uv run python models/fr_meteofrance/code/clim_upload.py [table_slug ...]

``quotidienne`` and ``mensuelle`` are directories of one parquet per
(département, period); ``poste`` is a single file. ``dicionario`` is re-uploaded
from the main output tree because adding the climatologiques code tables grew it
from 312 to 748 rows.

Every table is verified by row count against the parquet footers after upload,
and the run stops at the first mismatch rather than continuing.
"""

import os
import sys
from pathlib import Path

import google.cloud.storage as gcs
from basedosdados import Storage, Table
from google.cloud import bigquery

BILLING_PROJECT = "basedosdados-dev"
DATASET_ID = "fr_meteofrance"
STAGING_DATASET = f"{DATASET_ID}_staging"

CLIM_OUTPUT = Path(
    os.path.expanduser(
        os.environ.get("MFC_OUTPUT", "~/Downloads/fr_meteofrance_clim/output")
    )
)
MAIN_OUTPUT = Path(
    os.path.expanduser(
        os.environ.get(
            "MF_OUTPUT", "~/Downloads/fr_meteofrance_data/output_fr"
        )
    )
)

# The GCS bucket is requester-pays, so every bucket handle needs a billing project.
_orig_bucket = gcs.Client.bucket


def _patched_bucket(self, bucket_name, user_project=None):
    return _orig_bucket(self, bucket_name, user_project=BILLING_PROJECT)


gcs.Client.bucket = _patched_bucket

ROOTS = {
    "poste": CLIM_OUTPUT / "poste",
    "mensuelle": CLIM_OUTPUT / "mensuelle",
    "quotidienne": CLIM_OUTPUT / "quotidienne",
    "dicionario": MAIN_OUTPUT / "dicionario",
}
ORDER = ["dicionario", "poste", "mensuelle", "quotidienne"]


def expected_rows(root: Path) -> int:
    """Row count from the parquet footers, independent of BigQuery."""
    import pyarrow.parquet as pq

    files = sorted(root.rglob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"no parquet under {root}")
    return sum(pq.ParquetFile(f).metadata.num_rows for f in files)


def count_rows(slug: str) -> int:
    client = bigquery.Client(project=BILLING_PROJECT)
    query = f"SELECT COUNT(*) AS n FROM `{BILLING_PROJECT}.{STAGING_DATASET}.{slug}`"
    return next(iter(client.query(query).result())).n


def upload_one(slug: str) -> int:
    root = ROOTS[slug]
    expected = expected_rows(root)
    print(
        f"\n=== {slug} ===\npath: {root}\nexpected rows: {expected:,}",
        flush=True,
    )

    Storage(dataset_id=DATASET_ID, table_id=slug).delete_table(
        mode="staging", not_found_ok=True
    )
    print("staging prefix cleared", flush=True)

    Table(dataset_id=DATASET_ID, table_id=slug).create(
        path=str(root),
        source_format="parquet",
        if_table_exists="replace",
        if_storage_data_exists="replace",
    )
    print("staging table created and parquet uploaded", flush=True)

    got = count_rows(slug)
    print(f"row count in BigQuery: {got:,}", flush=True)
    if got != expected:
        raise AssertionError(
            f"{slug}: BigQuery has {got:,} rows, parquet has {expected:,}"
        )
    print(f"OK {slug}", flush=True)
    return got


def main():
    wanted = sys.argv[1:] or ORDER
    results = {slug: upload_one(slug) for slug in wanted}
    print("\n=== DONE ===")
    for slug, n in results.items():
        print(f"  {slug}: {n:,} rows")


if __name__ == "__main__":
    main()
