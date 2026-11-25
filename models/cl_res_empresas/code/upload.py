#!/usr/bin/env python3
"""Upload cl_res_empresas parquet to BigQuery staging.

uv run --no-project python models/cl_res_empresas/code/upload.py [--env dev|prod] [table ...]
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
DATASET_ID = "cl_res_empresas"
OUT = (
    Path(
        os.environ.get(
            "CL_RES_EMPRESAS_DATA",
            Path.home() / "Downloads" / "cl_res_empresas_data",
        )
    )
    / "output"
)

# smallest first
TABLES = ["dicionario", "sociedad"]

_orig_bucket = gcs.Client.bucket


def _patched_bucket(self, bucket_name, user_project=None):
    # Requester-pays bucket: force billing to our own project.
    return _orig_bucket(self, bucket_name, user_project=BILLING)


gcs.Client.bucket = _patched_bucket


def local_rows(slug):
    files = glob.glob(str(OUT / slug / "ano=*" / "*.parquet")) or glob.glob(
        str(OUT / slug / "*.parquet")
    )
    return sum(pq.ParquetFile(f).metadata.num_rows for f in files)


def upload(slug):
    path = OUT / slug
    if not path.exists():
        raise FileNotFoundError(path)
    expected = local_rows(slug)

    table = bd.Table(dataset_id=DATASET_ID, table_id=slug)
    storage = bd.Storage(dataset_id=DATASET_ID, table_id=slug)
    try:
        storage.delete_table(mode="staging", not_found_ok=True)
    except Exception as exc:
        print(f"  [warn] staging cleanup: {exc}")

    table.create(
        path=str(path),
        source_format="parquet",
        if_table_exists="replace",
        if_storage_data_exists="replace",
        if_dataset_exists="pass",
    )

    client = bigquery.Client(project=BILLING)
    got = next(
        iter(
            client.query(
                f"select count(*) n from `{BILLING}.{DATASET_ID}_staging.{slug}`"
            ).result()
        )
    ).n
    status = "OK" if got == expected else "MISMATCH"
    print(f"  {slug}: {got:,} rows (local {expected:,}) — {status}")
    if got != expected:
        raise ValueError(f"{slug}: BigQuery {got} != local {expected}")


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
