# -*- coding: utf-8 -*-
"""
Tasks for mundo_transfermarkt_competicoes_internacionais
"""

###############################################################################

import asyncio

import numpy as np
import pandas as pd
from prefect import task

from pipelines.datasets.mundo_transfermarkt_competicoes_internacionais.constants import (
    constants as mundo_constants,
)
from pipelines.datasets.mundo_transfermarkt_competicoes_internacionais.utils import (
    data_url,
    execucao_coleta,
)
from pipelines.utils.utils import extract_last_date, log, to_partitions


@task
def check_for_updates(dataset_id, table_id):
    """
    Checks if there are available updates for a specific dataset and table.

    Returns:
        bool: Returns True if updates are available, otherwise returns False.
    """
    # Obtém a data mais recente do site
    data_obj = data_url().strftime("%Y-%m-%d")

    # Obtém a última data no site BD
    data_bq_obj = extract_last_date(
        dataset_id, table_id, "yy-mm-dd", "basedosdados"
    ).strftime("%Y-%m-%d")

    # Registra a data mais recente do site
    log(f"Última data no site da transfermarkt: {data_obj}")
    log(f"Última data no site da BD: {data_bq_obj}")

    # Compara as datas para verificar se há atualizações
    if data_obj > data_bq_obj:
        return True  # Há atualizações disponíveis
    else:
        return False  # Não há novas atualizações disponíveis


@task
def execucao_coleta_sync():
    # Obter o loop de eventos atual e executar a tarefa nele
    loop = asyncio.get_event_loop()
    df = loop.run_until_complete(execucao_coleta())

    return df


@task
def make_partitions(df):
    log("Particionando os dados...")
    df["temporada"] = df["temporada"].astype(str)
    to_partitions(
        data=df,
        partition_columns=["temporada"],
        savepath="/tmp/data/mundo_transfermarkt_competicoes_internacionais/output/",
    )
    log("Dados particionados com sucesso!")
    return "/tmp/data/mundo_transfermarkt_competicoes_internacionais/output/"


@task
def get_max_data(df):
    df["data"] = pd.to_datetime(df["data"]).dt.date
    max_data = df["data"].max().strftime("%Y-%m-%d")
    log(max_data)
    return max_data
