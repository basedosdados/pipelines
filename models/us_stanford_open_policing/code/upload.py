"""Upload cleaned us_stanford_open_policing Parquet to BigQuery dev staging.

Targets ``basedosdados-dev`` only; prod tables are materialised by the merge.
Stops on the first table whose staging row count differs from the manifests
written by ``clean.py``.

Usage:
    python models/us_stanford_open_policing/code/upload.py [table ...] [--partial]
"""

import json
import sys
import warnings

warnings.filterwarnings("ignore")

import basedosdados as bd  # noqa: E402
import google.cloud.storage as gcs  # noqa: E402
from google.cloud import bigquery  # noqa: E402

try:
    from models.us_stanford_open_policing.code import constants
except ModuleNotFoundError:  # run as a script
    import constants  # type: ignore[no-redef]

BILLING_PROJECT = constants.BILLING_PROJECT
DATASET_ID = constants.DATASET_ID

_orig_bucket = gcs.Client.bucket


def _patched_bucket(self, bucket_name, user_project=None):
    return _orig_bucket(self, bucket_name, user_project=BILLING_PROJECT)


gcs.Client.bucket = _patched_bucket  # requester-pays bucket

TABLES = ["jurisdiction", "stop"]


def expected_rows(table: str, partial: bool = False) -> int:
    if table == "jurisdiction":
        return len(constants.JURISDICTIONS)
    manifests = sorted((constants.OUTPUT_DIR / "_manifest").glob("*.json"))
    if len(manifests) != len(constants.JURISDICTIONS) and not partial:
        raise RuntimeError(
            f"only {len(manifests)} of {len(constants.JURISDICTIONS)} jurisdictions cleaned"
        )
    return sum(json.loads(m.read_text())["rows"] for m in manifests)


def upload_table(table: str, partial: bool = False) -> int:
    path = constants.OUTPUT_DIR / table
    expected = expected_rows(table, partial)

    st = bd.Storage(dataset_id=DATASET_ID, table_id=table)
    try:
        st.delete_table(mode="staging", not_found_ok=True)
    except Exception as e:
        print(f"  [warn] staging prefix cleanup: {e}")

    bd.Table(dataset_id=DATASET_ID, table_id=table).create(
        path=str(path),
        source_format="parquet",
        if_table_exists="replace",
        if_storage_data_exists="replace",
        if_dataset_exists="pass",
    )

    client = bigquery.Client(project=BILLING_PROJECT)
    q = f"select count(*) as n from `{BILLING_PROJECT}.{DATASET_ID}_staging.{table}`"
    n = next(iter(client.query(q).result())).n
    status = "OK" if n == expected else "ROW MISMATCH"
    print(
        f"  {table}: staging {n:,} rows (expected {expected:,}) — {status}",
        flush=True,
    )
    if n != expected:
        raise ValueError(f"{table}: row count {n:,} != expected {expected:,}")
    return n


def main() -> None:
    partial = (
        "--partial" in sys.argv
    )  # trial runs before every file is cleaned
    only = [a for a in sys.argv[1:] if not a.startswith("--")]
    for table in [t for t in TABLES if not only or t in only]:
        print(f"=== {table} ===", flush=True)
        upload_table(table, partial)
    print("ALL TABLES UPLOADED")


if __name__ == "__main__":
    main()
