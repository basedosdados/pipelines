"""Upload the cleaned us_dol_oflc parquet tables to BigQuery.

Usage:
    uv run python models/us_dol_oflc/code/upload.py [--env dev|prod] [table ...]

--env dev (default) writes basedosdados-dev; --env prod writes basedosdados.
Point GOOGLE_APPLICATION_CREDENTIALS at the matching service account. Tables are
uploaded smallest first and the run stops on the first failure.
"""

import os
import sys
import warnings
from pathlib import Path

warnings.filterwarnings("ignore")

import basedosdados as bd  # noqa: E402
import google.cloud.storage as gcs  # noqa: E402
from google.cloud import bigquery  # noqa: E402

_argv = sys.argv[1:]
if "--env" in _argv:
    _i = _argv.index("--env")
    ENV = _argv[_i + 1]
    _argv = _argv[:_i] + _argv[_i + 2 :]
else:
    ENV = "dev"

BILLING_PROJECT = "basedosdados" if ENV == "prod" else "basedosdados-dev"
DATASET_ID = "us_dol_oflc"
DATA = Path(
    os.environ.get("OFLC_DATA_DIR", Path.home() / "Downloads/us_dol_oflc_data")
)
OUTPUT_ROOT = DATA / "output"

# The GCS bucket is requester-pays.
_orig_bucket = gcs.Client.bucket


def _patched_bucket(self, bucket_name, user_project=None):
    return _orig_bucket(self, bucket_name, user_project=BILLING_PROJECT)


gcs.Client.bucket = _patched_bucket

TABLES = ["dictionary", "h2b", "h2a", "perm", "lca"]


def upload_table(slug: str) -> int:
    path = OUTPUT_ROOT / slug
    if not path.exists():
        raise FileNotFoundError(f"Missing output path: {path}")

    st = bd.Storage(dataset_id=DATASET_ID, table_id=slug)
    try:
        st.delete_table(mode="staging", not_found_ok=True)
    except Exception as e:
        print(f"  [warn] staging prefix cleanup: {e}")

    tb = bd.Table(dataset_id=DATASET_ID, table_id=slug)
    tb.create(
        path=str(path),
        source_format="parquet",
        if_table_exists="replace",
        if_storage_data_exists="replace",
        if_dataset_exists="pass",
    )

    client = bigquery.Client(project=BILLING_PROJECT)
    q = f"select count(*) as n from `{BILLING_PROJECT}.{DATASET_ID}_staging.{slug}`"
    n = next(iter(client.query(q).result())).n
    print(f"  {slug}: {n:,} rows in staging")
    return n


def main() -> int:
    unknown = [a for a in _argv if a not in TABLES]
    if unknown:
        # Silently ignoring a typo would fall through to "all tables", which
        # deletes and replaces the staging data for every one of them.
        raise SystemExit(
            f"Unknown table argument(s): {', '.join(unknown)}. "
            f"Known tables: {', '.join(TABLES)}"
        )
    wanted = _argv or TABLES
    print(f"env={ENV} project={BILLING_PROJECT}")
    for slug in wanted:
        print(f"--- {slug} ---", flush=True)
        upload_table(slug)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
