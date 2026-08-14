"""Upload cleaned us_fed_fred parquet tables to BigQuery staging.

Usage:
    uv run python models/us_fed_fred/code/upload.py [--env dev|prod] [table_slug ...]

--env dev (default) -> basedosdados-dev; --env prod -> basedosdados. Point
GOOGLE_APPLICATION_CREDENTIALS at the matching service account. Reads the cleaned
parquet from $US_FED_FRED_DATA/output (default ~/Downloads/us_fed_fred_data/output),
which is NEVER under the repo or Dropbox. Uploads smallest table first and stops on
first failure.
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
DATASET_ID = "us_fed_fred"
DATA_ROOT = Path(
    os.environ.get(
        "US_FED_FRED_DATA", Path.home() / "Downloads" / "us_fed_fred_data"
    )
)
OUTPUT_ROOT = DATA_ROOT / "output"

# Monkey-patch for requester-pays bucket
_orig_bucket = gcs.Client.bucket


def _patched_bucket(self, bucket_name, user_project=None):
    return _orig_bucket(self, bucket_name, user_project=BILLING_PROJECT)


gcs.Client.bucket = _patched_bucket

# (table_slug, expected_rows) — smallest first
TABLES = [
    ("series", 50),
    ("observation", 171_801),
]


def upload_table(slug: str, expected_rows: int) -> int:
    path = OUTPUT_ROOT / slug
    if not path.exists():
        raise FileNotFoundError(f"Missing output path: {path}")

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

    client = bigquery.Client(project=BILLING_PROJECT)
    q = f"select count(*) as n from `{BILLING_PROJECT}.{DATASET_ID}_staging.{slug}`"
    n = next(iter(client.query(q).result())).n
    status = "OK" if n == expected_rows else "ROW MISMATCH"
    print(
        f"  {slug}: uploaded {n:,} rows (expected {expected_rows:,}) — {status}"
    )
    return n


def main() -> None:
    targets = _argv or [t[0] for t in TABLES]
    print(f"env={ENV} billing={BILLING_PROJECT} dataset={DATASET_ID}")
    for slug, expected in TABLES:
        if slug in targets:
            print(f"[upload] {slug}")
            upload_table(slug, expected)
    print("done.")


if __name__ == "__main__":
    main()
