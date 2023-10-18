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
    execucao_coleta,
)
from pipelines.utils.utils import log, to_partitions


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
