"""sobe o parquet limpo para o staging de basedosdados-dev.

uso: uv run models/br_bndes_fundos_garantidores/code/upload.py [tabela ...]
"""

import os
import sys
import warnings
from pathlib import Path

warnings.filterwarnings("ignore")

import basedosdados as bd  # noqa: E402
import google.cloud.storage as gcs  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402
from google.cloud import bigquery  # noqa: E402

BILLING_PROJECT = "basedosdados-dev"
DATASET_ID = "br_bndes_fundos_garantidores"
PASTA = Path(
    os.environ.get(
        "BNDES_FGI_DATA",
        Path.home() / "Downloads" / "br_bndes_fundos_garantidores_data",
    )
)
TABELAS = ["operacoes_fgi_tradicional"]

# bucket requester-pays
_bucket_original = gcs.Client.bucket


def _bucket(self, bucket_name, user_project=None):
    return _bucket_original(self, bucket_name, user_project=BILLING_PROJECT)


gcs.Client.bucket = _bucket


def subir(tabela: str) -> None:
    """recria o staging da tabela e confere a contagem de linhas."""
    pasta = PASTA / "output" / tabela
    esperado = sum(
        pq.read_metadata(p).num_rows for p in pasta.glob("ano=*/*.parquet")
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


if __name__ == "__main__":
    for tabela in sys.argv[1:] or TABELAS:
        subir(tabela)
