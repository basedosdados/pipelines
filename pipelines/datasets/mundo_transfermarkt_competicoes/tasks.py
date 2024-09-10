# -*- coding: utf-8 -*-
"""
Tasks for mundo_transfermarkt_competicoes
"""

import asyncio

import pandas as pd
from pandas import DataFrame
from prefect import task
from datetime import timedelta
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import re
from pipelines.constants import constants
from pipelines.datasets.mundo_transfermarkt_competicoes.utils import (
    execucao_coleta,
    execucao_coleta_copa,
    data_url,
)

from pipelines.datasets.mundo_transfermarkt_competicoes.constants import (
    constants as mundo_constants,
)
from pipelines.utils.utils import log, to_partitions

###############################################################################

@task(
    max_retries=1,
    retry_delay=timedelta(seconds=60),
)
def get_data_source_max_date() -> datetime:

    season = mundo_constants.SEASON.value
    base_url = f"https://www.transfermarkt.com/copa-do-brasil/gesamtspielplan/pokalwettbewerb/BRC/saison_id/{season}"

    headers = {
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36"
    }

    html = requests.get(base_url, headers=headers, timeout=120)

    soup = BeautifulSoup(html.text)

    pattern = r'\b[A-Za-z]{3}\s+\d{2},\s+\d{4}\b'

    datas = [re.findall(pattern, element.text)[0]
             for element in soup.select("tr:not([class]) td.hide-for-small")
             if re.findall(pattern, element.text)]

    ultima_data = max([datetime.strptime(data, "%b %d, %Y")
                       for data in datas
                       if datetime.strptime(data, "%b %d, %Y") <= datetime.today()])
    return ultima_data


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def get_data_source_transfermarkt_max_date():
    # Obtém a data mais recente do site
    data_obj = data_url().strftime("%Y-%m-%d")

    return data_obj


@task
def execucao_coleta_sync(tabela: str) -> pd.DataFrame:
    """
    Executa a coleta de dados de uma tabela especificada de forma síncrona.


    Args:
        tabela (str): O nome da tabela de dados a ser coletada. Deve ser 'brasileirao_serie_a' ou outro valor.

    Returns:
        pandas.DataFrame: Um DataFrame contendo os dados coletados da tabela especificada.
    """
    # Obter o loop de eventos atual e executar a tarefa nele
    loop = asyncio.get_event_loop()
    # Identifica a tabela
    if tabela == "brasileirao_serie_a":
        df = loop.run_until_complete(execucao_coleta())
    else:
        df = loop.run_until_complete(execucao_coleta_copa())
    return df


@task
def make_partitions(df: DataFrame) -> str:
    """
    Essa função adiciona uma coluna 'ano_campeonato' como string ao DataFrame 'df',
    particiona os dados com base nessa coluna e salva as partições em um diretório especificado.

    Args:
        df (pandas.DataFrame): O DataFrame de dados a ser particionado.

    Returns:
        str: O caminho para o diretório onde as partições foram salvas.
    """
    log("Particionando os dados...")
    df["ano_campeonato"] = df["ano_campeonato"].astype(str)
    to_partitions(
        data=df,
        partition_columns=["ano_campeonato"],
        savepath="/tmp/data/mundo_transfermarkt_competicoes/output/",
    )
    log("Dados particionados com sucesso!")
    return "/tmp/data/mundo_transfermarkt_competicoes/output/"
