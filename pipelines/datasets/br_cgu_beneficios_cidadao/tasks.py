# -*- coding: utf-8 -*-
"""
Tasks for br_cgu_beneficios_cidadao
"""

from datetime import timedelta

import pandas as pd
from prefect import task

from pipelines.datasets.br_cgu_beneficios_cidadao.constants import constants
from pipelines.datasets.br_cgu_beneficios_cidadao.utils import (
    download_unzip_csv,
    extract_dates,
    parquet_partition,
)
from pipelines.utils.utils import log
from typing import List, Union
from datetime import datetime, date


@task
def print_last_file(file):
    log(f"Arquivo baixado --> {file}")

@task
def scrape_download_page(table_id):
    dates = extract_dates(table=table_id)

    return dates

@task
def get_updated_files(files_df, table_last_date):
    files_df['ano_mes'] = files_df['ano'] + '-' + files_df['mes_numero'] + '-01'
    files_df['ano_mes'] = files_df['ano_mes'].apply(lambda x: datetime.strptime(x, "%Y-%m-%d").date())
    return files_df[files_df['ano_mes'] > table_last_date]['urls'].to_list()


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def get_source_max_date(files_df) -> list:
    """
    Encontra a data mais recente em um DataFrame e retorna a data e a URL correspondente.

    Parâmetros:
    - table_id (str): O identificador da tabela que contém os dados.

    Retorna:
    Uma lista contendo a data mais recente e a URL correspondente.

    Exemplo de uso:
    date, url = crawl_last_date("minha_tabela")
    """
    #dates = extract_dates(table=table_id)
    files_df["data"] = pd.to_datetime(
        files_df["ano"].astype(str) + "-" + files_df["mes_numero"].astype(str) + "-01"
    )
    max_row = files_df[files_df["data"] == files_df["data"].max()]

    max_date = max_row["data"].iloc[0]

    return [max_date, max_row["urls"].iloc[0]]


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)  # noqa
def crawler_beneficios_cidadao(table_id: str,
                               url: str,
                               files_df: pd.DataFrame,
                               historical_data: bool,
                               files: Union[List[str], None] = None,
                               year: str = "2023"
                               ) -> str:
    """
    Baixa e processa dados do portal da transparência relacionados aos benefícios do cidadão.

    Args:
        table_id (str): ID da tabela para identificar os dados.
        files_df (pd.DataFrame): DataFrame contendo informações sobre os arquivos a serem baixados.
        historical_data (bool): Indica se os dados históricos devem ser baixados.
        files (List[str], optional): Lista de URLs dos arquivos mais recentes, requeridos se historical_data for False.
        year (str, optional): Ano dos dados históricos a serem baixados.
        url (str): URL base para baixar os arquivos CSV.

    Returns:
        str: Caminho do diretório onde os dados processados foram armazenados.

    Raises:
        ValueError: Se historical_data for True e files for None.

    Note:
        Esta função baixa arquivos CSV relacionados aos benefícios do cidadão,
        opcionalmente filtrados por ano, e os processa em formato parquet.

    Example:
        >>> crawler_beneficios_cidadao(table_id='my_table',
        ...                            files_df=my_files_df,
        ...                            historical_data=True,
        ...                            url='https://example.com/beneficios_cidadao/')

    """
    if historical_data:

        endpoints = files_df[files_df["ano"] == year]["urls"].to_list()

        log("BAIXANDO DADOS HISTÓRICOS")
        log(f"ENDPOINTS >> {endpoints}")

        download_unzip_csv(
            url=url,
            files=endpoints,
            id=table_id,
        )
    else:
        log("BAIXANDO DADOS MAIS RECENTES")
        download_unzip_csv(
            url=url,
            files=files,
            id=table_id,
        )

    parquet_partition(
        path=f"/tmp/data/br_cgu_beneficios_cidadao/{table_id}/input/",
        table=table_id,
    )
    return f"/tmp/data/br_cgu_beneficios_cidadao/{table_id}/output/"