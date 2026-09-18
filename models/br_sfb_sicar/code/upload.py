"""Upload cleaned br_sfb_sicar parquet tables to BigQuery staging.

Usage:
    GOOGLE_APPLICATION_CREDENTIALS=~/.basedosdados/credentials/staging.json \
      uv run python models/br_sfb_sicar/code/upload.py [--env dev|prod] [table ...]

Data lives under $CAR_DATA/output (default ~/Downloads/br_sfb_sicar_data/output),
hive-partitioned by data (snapshot) + sigla_uf. Uploads smallest first, stops on
first failure, prints the staging row count per table.
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
DATASET_ID = "br_sfb_sicar"
OUTPUT_ROOT = (
    Path(
        os.environ.get(
            "CAR_DATA", str(Path.home() / "Downloads" / "br_sfb_sicar_data")
        )
    )
    / "output"
)

_orig_bucket = gcs.Client.bucket


def _patched_bucket(self, bucket_name, user_project=None):
    return _orig_bucket(self, bucket_name, user_project=BILLING_PROJECT)


gcs.Client.bucket = _patched_bucket

# smallest first
TABLES = [
    "dicionario",
    "uso_restrito",
    "area_pousio",
    "servidao_administrativa",
    "hidrografia",
    "vegetacao_nativa",
    "area_consolidada",
    "reserva_legal",
    "area_imovel",
    "app",
]


def upload_table(slug: str) -> int:
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
    print(f"  {slug}: {n:,} rows in staging", flush=True)
    return n


def main():
    only = set(_argv)
    tables = [t for t in TABLES if not only or t in only]
    print(
        f"=== uploading {tables} to {BILLING_PROJECT} (env={ENV}) ===",
        flush=True,
    )
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
