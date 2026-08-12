#!/usr/bin/env python3
"""Upload mx_sesnsp_incidencia_delictiva parquet (7 tables, ano-partitioned) to BigQuery.

Usage:
    uv run python models/mx_sesnsp_incidencia_delictiva/code/upload.py [--env dev|prod] [table ...]
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
BILLING = "basedosdados" if ENV == "prod" else "basedosdados-dev"
DATASET_ID = "mx_sesnsp_incidencia_delictiva"
OUT = (
    Path(
        os.environ.get(
            "MX_SESNSP_DATA",
            Path.home() / "Downloads" / "mx_sesnsp_incidencia_delictiva_data",
        )
    )
    / "output"
)

# smallest-first
TABLES = [
    "estatal_delitos",
    "estatal_delitos_2015_2025",
    "estatal_victimas",
    "estatal_victimas_2015_2025",
    "municipio_delitos",
    "municipio_victimas",
    "municipio_delitos_2015_2025",
]

_orig = gcs.Client.bucket
gcs.Client.bucket = lambda self, name, user_project=None: _orig(
    self, name, user_project=BILLING
)


def local_rows(slug):
    return sum(
        pq.ParquetFile(f).metadata.num_rows
        for f in glob.glob(str(OUT / slug / "ano=*" / "*.parquet"))
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
    print(f"=== upload {DATASET_ID} -> {BILLING} ===", flush=True)
    for slug in [t for t in TABLES if not only or t in only]:
        print(f"=== {slug} ===", flush=True)
        upload(slug)


if __name__ == "__main__":
    main()
