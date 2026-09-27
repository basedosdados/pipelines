"""Upload cleaned us_treasury_fiscaldata parquet tables to BigQuery staging.

Usage:
    uv run python models/us_treasury_fiscaldata/code/upload.py [--env dev|prod] [table ...]

--env dev (default) -> basedosdados-dev; --env prod -> basedosdados. Uploads
sequentially (smallest first) and stops on first failure. Scratch data root is
$US_TREASURY_DATA or ~/Downloads/us_treasury_fiscaldata_data.

Two monkeypatches: the GCS bucket is requester-pays (bill the target project),
and bd.Table.create reads one whole sample parquet into pandas just to list
column names — patched to read the footer instead, which also keeps the staging
table EXTERNAL (a load job would make it NATIVE and shadow the pipeline later).
"""

import os
import sys
import warnings
from pathlib import Path

warnings.filterwarnings("ignore")

import basedosdados as bd  # noqa: E402
import google.cloud.storage as gcs  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402
from basedosdados.upload import datatypes  # noqa: E402
from google.cloud import bigquery  # noqa: E402

_argv = sys.argv[1:]
if "--env" in _argv:
    _i = _argv.index("--env")
    ENV = _argv[_i + 1]
    _argv = _argv[:_i] + _argv[_i + 2 :]
else:
    ENV = "dev"
BILLING_PROJECT = "basedosdados" if ENV == "prod" else "basedosdados-dev"
DATASET_ID = "us_treasury_fiscaldata"
OUTPUT_ROOT = (
    Path(
        os.environ.get(
            "US_TREASURY_DATA",
            str(Path.home() / "Downloads" / "us_treasury_fiscaldata_data"),
        )
    )
    / "output"
)

# Requester-pays bucket: bill the target project.
_orig_bucket = gcs.Client.bucket


def _patched_bucket(self, bucket_name, user_project=None):
    return _orig_bucket(self, bucket_name, user_project=BILLING_PROJECT)


gcs.Client.bucket = _patched_bucket

# bd.Table.create reads a sample parquet fully into pandas just for column names;
# read the footer instead (keeps the EXTERNAL table, avoids the RAM blowup).
_orig_header = datatypes.Datatype.header


def _header(self, data_sample_path, csv_delimiter=","):
    if self.source_format == "parquet":
        p = Path(data_sample_path)
        if p.is_dir():
            p = next(iter(sorted(p.rglob("*.parquet"))))
        return list(pq.ParquetFile(str(p)).schema_arrow.names)
    return _orig_header(self, data_sample_path, csv_delimiter)


datatypes.Datatype.header = _header

# (table_slug, expected_rows) — smallest first
TABLES = [
    ("historical_debt_outstanding", 237),
    ("mts_summary", 3_117),
    ("average_interest_rate", 5_009),
    ("mts_means_of_financing", 6_759),
    ("mts_receipts", 7_604),
    ("debt_outstanding", 8_391),
    ("exchange_rate", 18_980),
    ("mts_outlays", 110_158),
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
    q = (
        f"select count(*) as n from "
        f"`{BILLING_PROJECT}.{DATASET_ID}_staging.{slug}`"
    )
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


def main():
    only = set(_argv)
    tables = [(s, r) for s, r in TABLES if not only or s in only]
    print(f"=== uploading to {BILLING_PROJECT} (env={ENV}) ===", flush=True)
    for slug, expected in tables:
        print(f"=== {slug} ===", flush=True)
        try:
            upload_table(slug, expected)
        except Exception as e:
            print(f"  FAILED: {type(e).__name__}: {e}")
            sys.exit(1)
    print("ALL TABLES UPLOADED")


if __name__ == "__main__":
    main()
