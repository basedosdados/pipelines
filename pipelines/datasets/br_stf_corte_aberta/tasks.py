# -*- coding: utf-8 -*-
"""
Tasks for br_stf_corte_aberta
"""

from prefect import task
import os
import pandas as pd
from pipelines.datasets.br_stf_corte_aberta.utils import (
    web_scrapping,
    read_csv,
    fix_columns_data,
    column_bool,
    rename_ordening_columns,
    replace_columns,
    partition_data,
)

from pipelines.utils.utils import extract_last_date, log
from pipelines.datasets.br_stf_corte_aberta.constants import constants as stf_constants


@task
def download_and_transform():
    log("Iniciando o web scrapping")
    web_scrapping()

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


@task
def make_partitions(df):
    partition_data(
        df=df, partition_columns="data_decisao", savepath=stf_constants.STF_OUTPUT.value
    )

    return stf_constants.STF_OUTPUT.value


@task
def check_for_data():
    web_scrapping()

    arquivos = os.listdir(stf_constants.STF_INPUT.value)
    for arquivo in arquivos:
        if arquivo.endswith(".csv"):
            df = pd.read_csv(stf_constants.STF_INPUT.value + arquivo, dtype=str)

    df["Data da decisão"] = df["Data da decisão"].astype(str).str[0:10]
    data_obj = df["Data da decisão"] = (
        df["Data da decisão"].astype(str).str[6:10]
        + "-"
        + df["Data da decisão"].astype(str).str[3:5]
        + "-"
        + df["Data da decisão"].astype(str).str[0:2]
    )
    data_obj = data_obj.max()

    return data_obj


@task
def check_for_updates(dataset_id, table_id):
    data_obj = check_for_data()
    # Obtém a última data no site BD
    data_bq_obj = extract_last_date(
        dataset_id, table_id, "yy-mm-dd", "basedosdados-dev", data="data_decisao"
    )

    # Registra a data mais recente do site
    log(f"Última data no site do ANP: {data_obj}")
    log(f"Última data no site da BD: {data_bq_obj}")

    # Compara as datas para verificar se há atualizações
    if data_obj > data_bq_obj:
        return True  # Há atualizações disponíveis
    else:
        return False  # Não há novas atualizações disponíveis
