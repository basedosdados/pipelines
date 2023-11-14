# -*- coding: utf-8 -*-
"""
Tasks for mundo_transfermarkt_competicoes
"""

import asyncio
import pandas as pd
from pandas import DataFrame
from prefect import task

###############################################################################

from pipelines.datasets.mundo_transfermarkt_competicoes.utils import (
    execucao_coleta,
    execucao_coleta_copa,
)
from pipelines.utils.utils import log, to_partitions


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


