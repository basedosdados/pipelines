# -*- coding: utf-8 -*-
"""
Tasks for mundo_transfermarkt_competicoes
"""

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
    df["ano_campeonato"] = df["ano_campeonato"].astype(str)
    to_partitions(
        data=df,
        partition_columns=["ano_campeonato"],
        savepath="/tmp/data/mundo_transfermarkt_competicoes/output/",
    )
    log("Dados particionados com sucesso!")
    return "/tmp/data/mundo_transfermarkt_competicoes/output/"


@task
def get_max_data(file_path):
    ano = mundo_constants.DATA_ATUAL_ANO.value
    df = pd.read_csv(f"{file_path}ano_campeonato={ano}/data.csv")
    df["data"] = pd.to_datetime(df["data"]).dt.date
    max_data = df["data"].max()

    # max_data = mundo_constants.DATA_ATUAL.value
    return max_data
