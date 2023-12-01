# -*- coding: utf-8 -*-
"""
Tasks for br_stf_corte_aberta
"""
from datetime import datetime, timedelta

from prefect import task

from pipelines.constants import constants
from pipelines.datasets.br_stf_corte_aberta.constants import constants as stf_constants
from pipelines.datasets.br_stf_corte_aberta.utils import (
    check_for_data,
    column_bool,
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
def get_data_source_stf_max_date():
    data_obj = check_for_data()
    data_obj = datetime.strptime(data_obj, "%Y-%m-%d").date()
    return data_obj


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
    return check_for_data()
