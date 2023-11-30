# -*- coding: utf-8 -*-
"""
Tasks for br_mg_belohorizonte_smfa_iptu
"""

import requests
from bs4 import BeautifulSoup
from prefect import task

from pipelines.datasets.br_mg_belohorizonte_smfa_iptu.constants import constants
from pipelines.datasets.br_mg_belohorizonte_smfa_iptu.utils import (
    changing_coordinates,
    concat_csv,
    fix_variables,
    new_column_endereco,
    new_columns_ano_mes,
    rename_columns,
    reorder_and_fix_nan,
    scrapping_download_csv,
)
from pipelines.utils.utils import  log, to_partitions


@task  # noqa
def download_and_transform():
    log("Iniciando o web scrapping e download dos arquivos csv")
    scrapping_download_csv(input_path=constants.INPUT_PATH.value)

    log("Iniciando a concatenação dos arquivos csv")
    df = concat_csv(input_path=constants.INPUT_PATH.value)

    log("Iniciando a renomeação das colunas")
    df = rename_columns(df=df)

    log("Iniciando a substituição de variáveis")
    df = fix_variables(df=df)

    log("Iniciando a criação da coluna endereço")
    df = new_column_endereco(df=df)

    log("Iniciando a criação das colunas ano e mes")
    df = new_columns_ano_mes(df=df)

    log("Iniciando a mudança de coordenadas")
    df = changing_coordinates(df=df)

    log("Iniciando a reordenação das colunas")
    df = reorder_and_fix_nan(df=df)

    return df


@task
def make_partitions(df):
    log("Iniciando a partição dos dados")

    to_partitions(
        data=df, partition_columns=["ano", "mes"], savepath=constants.OUTPUT_PATH.value
    )

    return constants.OUTPUT_PATH.value


def data_url(url, headers):
    response = requests.get(url, headers=headers)

    soup = BeautifulSoup(response.content, "html.parser")

    links = soup.find_all("a", href=lambda href: href and href.endswith(".csv"))

    if links:
        link = links[-1]
        filename = link.get("href").split("/")[-1][:6]
        data_final = filename[0:4] + "-" + filename[4:6]
        return data_final


@task
def get_data_source_sfma_iptu_max_date():
    # Obtém a data mais recente do site
    data_obj = data_url(constants.URLS.value[0], constants.HEADERS.value)
    return data_obj
