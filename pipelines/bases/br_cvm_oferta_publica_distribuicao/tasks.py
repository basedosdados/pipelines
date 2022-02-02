"""
Tasks for br_cvm_oferta_publica_distribuicao
"""

###############################################################################
#
# Aqui é onde devem ser definidas as tasks para os flows do projeto.
# Cada task representa um passo da pipeline. Não é estritamente necessário
# tratar todas as exceções que podem ocorrer durante a execução de uma task,
# mas é recomendável, ainda que não vá implicar em  uma quebra no sistema.
# Mais informações sobre tasks podem ser encontradas na documentação do
# Prefect: https://docs.prefect.io/core/concepts/tasks.html
#
# De modo a manter consistência na codebase, todo o código escrito passará
# pelo pylint. Todos os warnings e erros devem ser corrigidos.
#
# As tasks devem ser definidas como funções comuns ao Python, com o decorador
# @task acima. É recomendado inserir type hints para as variáveis.
#
# Um exemplo de task é o seguinte:
#
# -----------------------------------------------------------------------------
# from prefect import task
#
# @task
# def my_task(param1: str, param2: int) -> str:
#     """
#     My task description.
#     """
#     return f'{param1} {param2}'
# -----------------------------------------------------------------------------
#
# Você também pode usar pacotes Python arbitrários, como numpy, pandas, etc.
#
# -----------------------------------------------------------------------------
# from prefect import task
# import numpy as np
#
# @task
# def my_task(a: np.ndarray, b: np.ndarray) -> str:
#     """
#     My task description.
#     """
#     return np.add(a, b)
# -----------------------------------------------------------------------------
#
# Abaixo segue um código para exemplificação, que pode ser removido.
#
###############################################################################

import os
from pathlib import Path
from typing import Union

import pandas as pd
import basedosdados as bd
from prefect import task

from pipelines.utils import log

@task
def crawl(root: str, url: str) -> None:
    """Get table 'oferta_distribuicao' from CVM website"""
    filepath = f"{root}/oferta_distribuicao.csv"
    os.makedirs(root, exist_ok=True)

    # pylint: disable=invalid-name
    df: pd.DataFrame = pd.read_csv(url, encoding="latin-1", sep=";")
    df.to_csv(filepath, index=False, sep=";")

@task
def clean_table_oferta_distribuicao(root: str) -> str:
    # pylint: disable=invalid-name,no-member,unsubscriptable-object
    """Standardizes column names and selected variables"""
    in_filepath = f"{root}/oferta_distribuicao.csv"
    ou_filepath = f"{root}/br_cvm_oferta_publica_distribuicao.csv"

    df: pd.DataFrame = pd.read_csv(
        in_filepath,
        sep=";",
        keep_default_na=False,
        encoding="latin1",
        dtype=object,
    )

    df.columns = [k.lower() for k in df.columns]

    df.loc[(df["oferta_inicial"] == "N"), "oferta_inicial"] = "Não"
    df.loc[(df["oferta_inicial"] == "S"), "oferta_inicial"] = "Sim"

    df.loc[(df["oferta_incentivo_fiscal"] == "N"), "oferta_incentivo_fiscal"] = "Não"
    df.loc[(df["oferta_incentivo_fiscal"] == "S"), "oferta_incentivo_fiscal"] = "Sim"

    df.loc[(df["oferta_regime_fiduciario"] == "N"), "oferta_regime_fiduciario"] = "Não"
    df.loc[(df["oferta_regime_fiduciario"] == "S"), "oferta_regime_fiduciario"] = "Sim"

    df.to_csv(ou_filepath, index=False)

    return ou_filepath

@task
def upload_to_gcs(dataset_id: str, table_id: str, path: Union[str, Path]) -> None:
    # pylint: disable=invalid-name
    """Upload a bunch of CSVs to Google Cloud Storage using basedosdados library"""
    tb = bd.Table(dataset_id=dataset_id, table_id=table_id)

    if tb.table_exists(mode="staging"):
        tb.append(
            filepath=path,
            if_exists="replace",
        )

        log((f"Successfully uploaded {path} to "
             f"{tb.bucket_name}.staging.{dataset_id}.{table_id}"))
    else:
        log(("Table does not exist in STAGING, need to create it in local first.\n"
             "Create and publish the table in BigQuery first."))
