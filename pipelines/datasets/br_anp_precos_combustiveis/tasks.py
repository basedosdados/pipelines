# -*- coding: utf-8 -*-
"""
Tasks for br_anp_precos_combustiveis
"""

from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from prefect import task

from pipelines.constants import constants
from pipelines.datasets.br_anp_precos_combustiveis.constants import (
    constants as anatel_constants,
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
from pipelines.utils.utils import extract_last_date, log


@task
def check_for_updates(dataset_id, table_id):
    """
    Checks if there are available updates for a specific dataset and table.

    Returns:
        bool: Returns True if updates are available, otherwise returns False.
    """
    # Obtém a data mais recente do site
    download_files(anatel_constants.URLS_DATA.value, anatel_constants.PATH_INPUT.value)
    df = pd.read_csv(anatel_constants.URL_GLP.value, sep=";", encoding="utf-8")
    data_obj = pd.to_datetime(df["Data da Coleta"]).max()
    data_obj = data_obj.date()

    # Obtém a última data no site BD
    data_bq_obj = extract_last_date(
        dataset_id, table_id, "yy-mm-dd", "basedosdados", data="data_coleta"
    )

    # Registra a data mais recente do site
    log(f"Última data no site do ANP: {data_obj}")
    log(f"Última data no site da BD: {data_bq_obj}")

    # Compara as datas para verificar se há atualizações
    if data_obj > data_bq_obj:
        return True  # Há atualizações disponíveis
    else:
        return False  # Não há novas atualizações disponíveis


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def download_and_transform():
    download_files(anatel_constants.URLS.value, anatel_constants.PATH_INPUT.value)

    precos_combustiveis = open_csvs(
        anatel_constants.URL_DIESEL_GNV.value,
        anatel_constants.URL_GASOLINA_ETANOL.value,
        anatel_constants.URL_GLP.value,
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
        output_directory=anatel_constants.PATH_OUTPUT.value,
    )
    return anatel_constants.PATH_OUTPUT.value


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def data_max_bd_pro(df):
    max_value = pd.to_datetime(df["data_coleta"]).max()
    return max_value.strftime("%Y-%m-%d")


def data_max_bd_mais():
    data_referencia = datetime.now() - pd.DateOffset(weeks=6)
    return data_referencia.strftime("%Y-%m-%d")
