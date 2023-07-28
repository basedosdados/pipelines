# -*- coding: utf-8 -*-
"""
Tasks for mundo_transfermarkt_competicoes
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
from pipelines.datasets.mundo_transfermarkt_competicoes.constants import (
    constants as mundo_constants,
)
from pipelines.utils.utils import log, to_partitions
from prefect import task
import re
import numpy as np
import pandas as pd
import asyncio
from datetime import timedelta, datetime


@task
def execucao_coleta_sync(execucao_coleta):
    # Obter o loop de eventos atual e executar a tarefa nele
    loop = asyncio.get_event_loop()
    df = loop.run_until_complete(execucao_coleta())

    return df


@task
def make_partitions(df):
    log("Particionando os dados...")
    to_partitions(
        data=df,
        partition_columns=["ano_campeonato"],
        savepath="/tmp/data/mundo_transfermarkt_competicoes/output/",
    )
    log("Dados particionados com sucesso!")
    return "/tmp/data/mundo_transfermarkt_competicoes/output/"


@task
def get_max_data():
    # ano = mundo_constants.DATA_ATUAL_ANO.value
    # df = pd.read_csv(f"{file_path}ano_campeonato={ano}/data.csv")
    # df["data"] = pd.to_datetime(df["data"]).dt.date
    max_data = mundo_constants.DATA_ATUAL.value
    max_data = datetime.strptime(max_data, "%Y-%m-%d").date()
    
    return max_data