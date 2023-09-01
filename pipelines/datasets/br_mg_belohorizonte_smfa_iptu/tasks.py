# -*- coding: utf-8 -*-
"""
Tasks for br_mg_belohorizonte_smfa_iptu
"""
from prefect import task
import pandas as pd
from pipelines.datasets.br_mg_belohorizonte_smfa_iptu.constants import constants
from pipelines.utils.utils import to_partitions
import os
from pipelines.datasets.br_mg_belohorizonte_smfa_iptu.utils import (
    scrapping_download_csv,
    concat_csv,
    rename_columns,
    replace_variables,
    new_columns_endereco,
    new_columns_ano_mes,
    reordering_and_np_nan,
)


@task  # noqa
def tasks_pipeline():
    scrapping_download_csv(constants.INPUT.value)

    concat_csv_result = concat_csv(constants.INPUT.value)

    rename_columns_result = rename_columns(df=concat_csv_result)

    replace_variables_result = replace_variables(df=rename_columns_result)

    new_columns_endereco_result = new_columns_endereco(df=replace_variables_result)

    new_columns_ano_mes_result = new_columns_ano_mes(df=new_columns_endereco_result)

    df_reordering = reordering_and_np_nan(df=new_columns_ano_mes_result)

    return df_reordering


@task
def make_partitions(df):
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
