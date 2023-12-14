# -*- coding: utf-8 -*-
"""
Tasks for br_anp_precos_combustiveis
"""

from datetime import timedelta

import pandas as pd
from prefect import task

from pipelines.constants import constants
from pipelines.datasets.br_anp_precos_combustiveis.constants import (
    constants as anp_constants,
)
from pipelines.datasets.br_anp_precos_combustiveis.utils import (
    creating_column_ano,
    download_files,
    get_id_municipio,
    lower_colunm_produto,
    merge_table_id_municipio,
    open_csvs,
    orderning_data_coleta,
    partition_data,
    rename_and_reordening,
    rename_and_to_create_endereco,
    rename_columns,
)


@task
def get_data_source_anp_max_date():
    # Obtém a data mais recente do site
    download_files(anp_constants.URLS_DATA.value, anp_constants.PATH_INPUT.value)
    df = pd.read_csv(anp_constants.URL_GLP.value, sep=";", encoding="utf-8")
    data_obj = (
        df["Data da Coleta"].str[6:10]
        + "-"
        + df["Data da Coleta"].str[3:5]
        + "-"
        + df["Data da Coleta"].str[0:2]
    )
    data_obj = data_obj.apply(lambda x: pd.to_datetime(x).strftime("%Y-%m-%d"))
    data_obj = pd.to_datetime(data_obj, format="%Y-%m-%d").dt.date
    data_obj = data_obj.max()
    return data_obj


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def download_and_transform():
    download_files(anp_constants.URLS.value, anp_constants.PATH_INPUT.value)

    precos_combustiveis = open_csvs(
        anp_constants.URL_DIESEL_GNV.value,
        anp_constants.URL_GASOLINA_ETANOL.value,
        anp_constants.URL_GLP.value,
    )

    df = get_id_municipio(id_municipio=precos_combustiveis)

    df = merge_table_id_municipio(
        id_municipio=df, pd_precos_combustiveis=precos_combustiveis
    )

    df = rename_and_to_create_endereco(precos_combustiveis=df)

    df = orderning_data_coleta(precos_combustiveis=df)

    df = lower_colunm_produto(precos_combustiveis=df)

    df = creating_column_ano(precos_combustiveis=df)

    df = rename_and_reordening(precos_combustiveis=df)

    df = rename_columns(precos_combustiveis=df)

    return df


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def make_partitions(df):
    partition_data(
        df,
        column_name="data_coleta",
        output_directory=anp_constants.PATH_OUTPUT.value,
    )
    return anp_constants.PATH_OUTPUT.value
