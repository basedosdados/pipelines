"""sobe o parquet limpo para o staging de basedosdados-dev.

uso: uv run models/br_bndes_fundos_garantidores/code/upload.py [tabela ...]
(sem argumento, sobe as 6 tabelas)
"""

import os
import sys
from pathlib import Path

import basedosdados as bd
import google.cloud.storage as gcs
import pyarrow.parquet as pq
from google.cloud import bigquery

from pipelines.datasets.br_bndes_fundos_garantidores.constants import (
    constants,
)

BILLING_PROJECT = "basedosdados-dev"
DATASET_ID = "br_bndes_fundos_garantidores"
PASTA = Path(
    os.environ.get(
        "BNDES_FGI_DATA",
        Path.home() / "Downloads" / "br_bndes_fundos_garantidores_data",
    )
)
TABELAS = list(constants.NOME_CENTRAL.value)


def subir(tabela: str) -> None:
    """recria o staging da tabela e confere a contagem de linhas."""
    pasta = PASTA / "output" / tabela
    esperado = sum(
        pq.read_metadata(p).num_rows for p in pasta.glob("ano=*/*.parquet")
    )

    # só apaga e recria se a configuração local apontar para o dev
    destino = bd.Table(dataset_id=DATASET_ID, table_id=tabela)
    projeto = destino.config.unwrap()["gcloud-projects"]["staging"]["name"]
    if (destino.bucket_name, projeto) != (BILLING_PROJECT, BILLING_PROJECT):
        raise ValueError(
            f"config aponta para {destino.bucket_name}/{projeto}, não {BILLING_PROJECT}"
        )

    bd.Storage(dataset_id=DATASET_ID, table_id=tabela).delete_table(
        mode="staging", not_found_ok=True
    )
    bd.Table(dataset_id=DATASET_ID, table_id=tabela).create(
        path=str(pasta),
        source_format="parquet",
        if_table_exists="replace",
        if_storage_data_exists="replace",
        if_dataset_exists="pass",
    )

    client = bigquery.Client(project=BILLING_PROJECT)
    sql = f"select count(*) n from `{BILLING_PROJECT}.{DATASET_ID}_staging.{tabela}`"
    n = next(iter(client.query(sql).result())).n
    print(f"{tabela}: {n:,} linhas no staging (esperado {esperado:,})")
    if n != esperado:
        raise ValueError(f"{tabela}: {n} != {esperado}")


def main(tabelas: list[str]) -> None:
    """sobe as tabelas com o bucket requester-pays cobrado no dev."""
    original = gcs.Client.bucket

    def _bucket(self, bucket_name, user_project=None):
        return original(self, bucket_name, user_project=BILLING_PROJECT)

    gcs.Client.bucket = _bucket
    try:
        for tabela in tabelas:
            subir(tabela)
    finally:
        gcs.Client.bucket = original


if __name__ == "__main__":
    main(sys.argv[1:] or TABELAS)
