# -*- coding: utf-8 -*-
"""
Tasks for br_mg_belohorizonte_smfa_iptu
"""
from prefect import task
import pandas as pd
from pipelines.utils.tasks import log
from pipelines.datasets.br_mg_belohorizonte_smfa_iptu.constants import constants
from pipelines.utils.utils import to_partitions
import os
from pipelines.datasets.br_mg_belohorizonte_smfa_iptu.utils import (
    scrapping_download_csv,
    concat_csv,
    rename_columns,
    fix_variables,
    new_column_endereco,
    new_columns_ano_mes,
    reorder_and_fix_nan,
    changing_coordinates,
)


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


@task
def get_max_data(input):
    arquivos = os.listdir(input)
    valor = [valor[0:6] for valor in arquivos]
    resultado = valor[0]
    data = resultado[0:4] + "-" + resultado[4:6]
    return data
