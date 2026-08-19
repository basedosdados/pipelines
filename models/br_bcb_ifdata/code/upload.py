"""Upload de onboarding do br_bcb_ifdata para o staging do BigQuery dev.

Envia o parquet all-STRING para `basedosdados-dev.br_bcb_ifdata_staging.<tabela>`
e confere a contagem de linhas contra o que existe em disco.

Duas escolhas que valem explicação:

* **All-STRING, não tipado.** É a convenção da casa para staging — o modelo dbt
  faz o `safe_cast` de cada coluna. Manter o upload do onboarding e o do
  pipeline recorrente no mesmo formato evita a divergência de esquema descrita
  em `prefect-pipeline-conventions` (tabela externa tipada sobrevivendo a um
  overwrite all-STRING).

* **Sem hive partitioning.** O `ano` já é uma coluna dentro do parquet, então
  ligar o hive partitioning sobre `ano=<AAAA>/` declararia a mesma coluna duas
  vezes, com tipos diferentes (STRING no arquivo, INT64 inferido do caminho). O
  particionamento de verdade é o do modelo dbt; o caminho no GCS é só
  organização.

Billing / requester-pays: basedosdados-dev. Autenticação: ADC.
"""

from __future__ import annotations

import argparse
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

import google.cloud.storage as gcs
import pyarrow.parquet as pq
from google.cloud import bigquery

BILLING_PROJECT = "basedosdados-dev"
BUCKET = "basedosdados-dev"
GCP_DATASET_ID = "br_bcb_ifdata"
STAGING_DATASET = "br_bcb_ifdata_staging"
DATA_ROOT = os.environ.get(
    "IFDATA_OUTPUT",
    os.path.expanduser("~/Downloads/br_bcb_ifdata_data/output"),
)
TABLES = ["instituicao", "coluna", "relatorio", "dicionario"]

_orig_bucket = gcs.Client.bucket


def _patched_bucket(self, bucket_name, user_project=None):
    return _orig_bucket(self, bucket_name, user_project=BILLING_PROJECT)


gcs.Client.bucket = _patched_bucket

storage_client = gcs.Client(project=BILLING_PROJECT)
bq = bigquery.Client(project=BILLING_PROJECT)


def ensure_dataset() -> None:
    ds = bigquery.Dataset(f"{BILLING_PROJECT}.{STAGING_DATASET}")
    ds.location = "US"
    bq.create_dataset(ds, exists_ok=True)
    print(f"  dataset {BILLING_PROJECT}.{STAGING_DATASET} pronto")


def delete_prefix(prefix: str) -> None:
    bucket = storage_client.bucket(BUCKET)
    blobs = list(storage_client.list_blobs(bucket, prefix=prefix))
    if not blobs:
        print(f"  sem blobs antigos em {prefix}")
        return
    for i in range(0, len(blobs), 500):
        bucket.delete_blobs(blobs[i : i + 500])
    print(f"  removidos {len(blobs)} blobs antigos em {prefix}")


def local_files(name: str) -> list[tuple[str, str]]:
    root = os.path.join(DATA_ROOT, name)
    out = []
    for dirpath, _, filenames in os.walk(root):
        for fn in filenames:
            if fn.endswith(".parquet"):
                full = os.path.join(dirpath, fn)
                out.append((full, os.path.relpath(full, root)))
    return sorted(out)


def local_rows(files: list[tuple[str, str]]) -> int:
    """Linhas em disco, lidas do metadado do parquet (não carrega os dados)."""
    return sum(pq.ParquetFile(f).metadata.num_rows for f, _ in files)


def _upload_one(bucket, prefix, full, rel) -> None:
    blob = bucket.blob(prefix + rel.replace(os.sep, "/"))
    blob.chunk_size = 256 * 1024 * 1024
    for attempt in range(3):
        try:
            blob.upload_from_filename(full)
            return
        except Exception:
            if attempt == 2:
                raise


def upload_files(files, prefix: str) -> None:
    bucket = storage_client.bucket(BUCKET)
    print(f"  enviando {len(files)} arquivos -> gs://{BUCKET}/{prefix}")
    done = 0
    with ThreadPoolExecutor(max_workers=16) as ex:
        futs = [
            ex.submit(_upload_one, bucket, prefix, full, rel)
            for full, rel in files
        ]
        for fut in as_completed(futs):
            fut.result()
            done += 1
            if done % 50 == 0 or done == len(files):
                print(f"    {done}/{len(files)}")


def load_table(name: str, prefix: str) -> str:
    table_id = f"{BILLING_PROJECT}.{STAGING_DATASET}.{name}"
    jc = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    job = bq.load_table_from_uri(
        f"gs://{BUCKET}/{prefix}*.parquet", table_id, job_config=jc
    )
    job.result()
    print(f"  load job {job.job_id}: output_rows={job.output_rows:,}")
    return table_id


def process(name: str) -> tuple[str, int]:
    files = local_files(name)
    if not files:
        print(f"\n=== {name}: NENHUM parquet em disco, pulando ===")
        return name, 0
    esperado = local_rows(files)
    print(f"\n=== {name} ({len(files)} arquivos, {esperado:,} linhas) ===")
    prefix = f"staging/{GCP_DATASET_ID}/{name}/"
    delete_prefix(prefix)
    upload_files(files, prefix)
    table_id = load_table(name, prefix)
    n = next(iter(bq.query(f"SELECT COUNT(*) n FROM `{table_id}`").result())).n
    print(
        f"  COUNT(*)={n:,} esperado={esperado:,} bate={'S' if n == esperado else 'N'}"
    )
    if n != esperado:
        print(f"!! DIVERGÊNCIA em {name}: {n} != {esperado}")
        sys.exit(1)
    return name, n


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--only", default=None, help="tabelas separadas por vírgula"
    )
    args = ap.parse_args()
    only = set(args.only.split(",")) if args.only else None

    ensure_dataset()
    res = [process(t) for t in TABLES if not only or t in only]
    print("\n=== RESUMO ===")
    for name, n in res:
        print(f"  {name:<12} {n:>12,}")


if __name__ == "__main__":
    main()
