"""
Upload one year's partitioned CSVs to the dev staging bucket, replacing that
year's prefix only.

The staging external tables map CSV columns BY POSITION, so every file is
reordered to the live staging schema (read from BigQuery) before upload, and
the upload refuses any file whose columns are not exactly that schema.

Usage:
    TSE_DATA_DIR=... python -m models.br_tse_eleicoes.code.python.upload_year 2026 table [table ...]
"""

import os
import sys
from pathlib import Path

import pandas as pd
from google.cloud import bigquery, storage
from google.oauth2 import service_account

from models.br_tse_eleicoes.code.python.config import OUTPUT_PYTHON

PROJECT = "basedosdados-dev"
BUCKET = "basedosdados-dev"
DATASET = "br_tse_eleicoes"
HIVE = {"ano", "sigla_uf"}
CRED = Path(
    os.environ.get(
        "BD_SERVICE_ACCOUNT_DEV",
        Path.home() / ".basedosdados/credentials/staging.json",
    )
)


def _staging_schema(bq: bigquery.Client, table: str) -> list[str]:
    q = f"""SELECT column_name FROM `{PROJECT}.{DATASET}_staging.INFORMATION_SCHEMA.COLUMNS`
            WHERE table_name = @t ORDER BY ordinal_position"""
    job = bq.query(
        q,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("t", "STRING", table)
            ]
        ),
    )
    return [r.column_name for r in job.result() if r.column_name not in HIVE]


def upload(ano: int, tables: list[str], dry_run: bool = False) -> None:
    cred = service_account.Credentials.from_service_account_file(str(CRED))
    bq = bigquery.Client(project=PROJECT, credentials=cred)
    bucket = storage.Client(project=PROJECT, credentials=cred).bucket(
        BUCKET, user_project=PROJECT
    )
    for table in tables:
        cols = _staging_schema(bq, table)
        files = sorted((OUTPUT_PYTHON / table / f"ano={ano}").rglob("*.csv"))
        if not files:
            print(f"{table}: no files for ano={ano}, skipped")
            continue
        payloads = []
        for f in files:
            header = list(pd.read_csv(f, nrows=0).columns)
            if set(header) != set(cols):
                msg = (
                    f"{f}: columns differ from staging {table}. "
                    f"missing={set(cols) - set(header)} "
                    f"extra={set(header) - set(cols)}"
                )
                raise ValueError(msg)
            src = f
            if header != cols:  # reorder to the staging position order
                src = f.with_suffix(".staging.csv")
                first = True
                for chunk in pd.read_csv(
                    f, dtype=str, keep_default_na=False, chunksize=1_000_000
                ):
                    chunk[cols].to_csv(
                        src,
                        index=False,
                        header=first,
                        mode="w" if first else "a",
                    )
                    first = False
            rel = f.relative_to(OUTPUT_PYTHON / table).as_posix()
            payloads.append((f"staging/{DATASET}/{table}/{rel}", src))
        size = sum(p.stat().st_size for _, p in payloads) / 1e6
        print(
            f"{table}: {len(files)} files, {size:,.0f} MB, reordered={payloads[0][1] != files[0]}"
        )
        if dry_run:
            continue
        prefix = f"staging/{DATASET}/{table}/ano={ano}/"
        stale = list(bucket.list_blobs(prefix=prefix))
        for blob in stale:
            blob.delete()
        for name, src in payloads:
            bucket.blob(name).upload_from_filename(
                str(src), content_type="text/csv", timeout=3600
            )
        print(f"  replaced {len(stale)} old blobs under {prefix}")


if __name__ == "__main__":
    args = sys.argv[1:]
    dry = "--dry-run" in args
    args = [a for a in args if a != "--dry-run"]
    upload(int(args[0]), args[1:], dry_run=dry)
