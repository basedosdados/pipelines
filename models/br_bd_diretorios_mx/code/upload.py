#!/usr/bin/env python3
"""Upload br_bd_diretorios_mx parquet (estado, municipio) to BigQuery staging.

Usage:
    uv run python models/br_bd_diretorios_mx/code/upload.py [--env dev|prod] [table ...]
"""

import glob
import os
import sys
import warnings
from pathlib import Path

warnings.filterwarnings("ignore")
import basedosdados as bd  # noqa: E402
import google.cloud.storage as gcs  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402
from google.cloud import bigquery  # noqa: E402

_argv = sys.argv[1:]
ENV = "dev"
if "--env" in _argv:
    i = _argv.index("--env")
    ENV = _argv[i + 1]
    _argv = _argv[:i] + _argv[i + 2 :]
if ENV not in ("dev", "prod"):
    sys.exit(f"--env must be 'dev' or 'prod', got {ENV!r}")
BILLING = "basedosdados" if ENV == "prod" else "basedosdados-dev"
DATASET_ID = "br_bd_diretorios_mx"
OUT = (
    Path(
        os.environ.get(
            "MX_DIR_DATA",
            Path.home() / "Downloads" / "br_bd_diretorios_mx_data",
        )
    )
    / "output"
)
TABLES = ["estado", "municipio"]

_orig_bucket = gcs.Client.bucket


def _patched_bucket(self, bucket_name, user_project=None):
    # Requester-pays: force billing to our project; matches the pinned
    # google-cloud-storage Client.bucket signature (bucket_name, user_project).
    return _orig_bucket(self, bucket_name, user_project=BILLING)


gcs.Client.bucket = _patched_bucket


def local_rows(slug):
    return sum(
        pq.ParquetFile(f).metadata.num_rows
        for f in glob.glob(str(OUT / slug / "*.parquet"))
    )


def upload(slug):
    path = OUT / slug
    if not path.exists():
        raise FileNotFoundError(path)
    expected = local_rows(slug)
    tb = bd.Table(dataset_id=DATASET_ID, table_id=slug)
    st = bd.Storage(dataset_id=DATASET_ID, table_id=slug)
    try:
        st.delete_table(mode="staging", not_found_ok=True)
    except Exception as e:
        print(f"  [warn] staging cleanup: {e}")
    tb.create(
        path=str(path),
        source_format="parquet",
        if_table_exists="replace",
        if_storage_data_exists="replace",
        if_dataset_exists="pass",
    )
    try:
        c = bigquery.Client(project=BILLING)
        n = next(
            iter(
                c.query(
                    f"select count(*) n from `{BILLING}.{DATASET_ID}_staging.{slug}`"
                ).result()
            )
        ).n
        print(
            f"  {slug}: {n:,} rows (local {expected:,}) — {'OK' if n == expected else 'MISMATCH'}"
        )
        if n != expected:
            raise ValueError(f"{slug}: BQ {n} != local {expected}")
    except ValueError:
        raise
    except Exception as e:
        print(
            f"  {slug}: staging created; local {expected:,} rows (BQ unverified: {type(e).__name__})"
        )


def main():
    only = set(_argv)
    unknown = only - set(TABLES)
    if unknown:
        sys.exit(f"unknown table(s): {sorted(unknown)}")
    print(f"=== upload {DATASET_ID} -> {BILLING} ===", flush=True)
    for slug in [t for t in TABLES if not only or t in only]:
        print(f"=== {slug} ===", flush=True)
        upload(slug)


if __name__ == "__main__":
    main()
