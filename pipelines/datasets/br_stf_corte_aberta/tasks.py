# -*- coding: utf-8 -*-
"""
Tasks for br_stf_corte_aberta
"""
from datetime import timedelta
import datetime

import pandas as pd
from prefect import task

from pipelines.constants import constants
from pipelines.datasets.br_stf_corte_aberta.constants import constants as stf_constants
from pipelines.datasets.br_stf_corte_aberta.utils import (
    # check_for_data,
    column_bool,
    extract_last_date,
    fix_columns_data,
    partition_data,
    read_csv,
    rename_ordening_columns,
    replace_columns,
)
from pipelines.utils.utils import log


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def check_for_updates(dataset_id, table_id):
    log('check_for_updates')
    data_obj = '2023-10-18' #check_for_data()
    data_obj = datetime.strptime(data_obj, "%Y-%m-%d").date()
    # Obtém a última data no site BD
    data_bq_obj = extract_last_date(
        dataset_id=dataset_id,
        table_id=table_id,
        date_format="yy-mm-dd",
        billing_project_id="basedosdados",
        data="data_decisao",
    )
    # Registra a data mais recente do site
    log(f"Última data no site do STF: {data_obj}")
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
# make_partition
def make_partitions(df):
    partition_data(df, "data_decisao", stf_constants.STF_OUTPUT.value)
    return stf_constants.STF_OUTPUT.value


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def download_and_transform():
    log("Iniciando a leitura do csv")
    df = read_csv()
    log("Iniciando a correção das colunas de data")
    df = fix_columns_data(df)
    log("Iniciando a correção da coluna booleana")
    df = column_bool(df)
    log("Iniciando a renomeação e ordenação das colunas")
    df = rename_ordening_columns(df)
    log("Iniciando a substituição de variáveis")
    df = replace_columns(df)
    return df

@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def task_check_for_data():
    return '2023-10-18'  #check_for_data()