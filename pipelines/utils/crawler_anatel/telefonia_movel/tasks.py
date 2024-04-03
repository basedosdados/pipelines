# -*- coding: utf-8 -*-
"""
Tasks for dataset br_anatel_telefonia_movel
"""

import os
from datetime import timedelta
import numpy as np
import pandas as pd
from prefect import task
from pipelines.constants import constants
from pipelines.utils.crawler_anatel.telefonia_movel.constants import (
    constants as anatel_constants,
)
from pipelines.utils.crawler_anatel.telefonia_movel.utils import (
    unzip_file,
)
from pipelines.utils.utils import log, to_partitions


# ! TASK MICRODADOS
def clean_csv_microdados(ano, semestre, table_id):
    log("Download dos dados...")
    log(anatel_constants.URL.value)
    os.system(f"mkdir -p {anatel_constants.INPUT_PATH.value}")

    df = pd.read_csv(
        f"{anatel_constants.INPUT_PATH.value}Acessos_Telefonia_Movel_{ano}_{semestre}S.csv",
        sep=";",
        encoding="utf-8",
    )


    log("Renomeando as colunas:")


    df.rename(columns=anatel_constants.RENAME_COLUMNS_MICRODADOS.value, inplace=True)

    df.drop(anatel_constants.DROP_COLUMNS_MICRODADOS.value, axis=1, inplace=True)

    df["produto"] = df["produto"].str.lower()


    df["id_municipio"] = df["id_municipio"].astype(str)


    df["ddd"] = pd.to_numeric(df["ddd"], downcast="integer").astype(str)

    df["cnpj"] = df["cnpj"].astype(str)

    df = df[anatel_constants.ORDER_COLUMNS_MICRODADOS.value]

    to_partitions(
        df,
        partition_columns=["ano", "mes"],
        savepath=anatel_constants.TABLES_OUTPUT_PATH.value[table_id],
    )


# ! TASK BRASIL
def clean_csv_brasil(table_id):

    log("Abrindo os dados do Brasil...")

    densidade = pd.read_csv(
        f"{anatel_constants.INPUT_PATH.value}Densidade_Telefonia_Movel.csv",
        sep=";",
        encoding="utf-8",
    )

    densidade.rename(columns={"Nível Geográfico Densidade": "geografia"}, inplace=True)

    densidade_brasil = densidade[densidade["geografia"] == "Brasil"]

    densidade_brasil = densidade_brasil[anatel_constants.ORDER_COLUMNS_BRASIL.value]

    densidade_brasil = densidade_brasil.rename(
        columns=anatel_constants.RENAME_COLUMNS_BRASIL.value
    )

    densidade_brasil["densidade"] = (
        densidade_brasil["densidade"].astype(str).str.replace(",", ".").astype(float)
    )
    log("Salvando os dados do Brasil...")

    densidade_brasil.to_csv(
        f"{anatel_constants.TABLES_OUTPUT_PATH.value[table_id]}densidade_brasil.csv",
        index=False,
        sep=",",
        encoding="utf-8",
        na_rep="",
    )


# ! TASK UF
def clean_csv_uf(table_id):
    log("Abrindo os dados por UF...")
    densidade = pd.read_csv(
        f"{anatel_constants.INPUT_PATH.value}Densidade_Telefonia_Movel.csv",
        sep=";",
        encoding="utf-8",
    )

    densidade.rename(columns={"Nível Geográfico Densidade": "geografia"}, inplace=True)

    densidade_uf = densidade[densidade["geografia"] == "UF"]

    densidade_uf = densidade_uf[anatel_constants.ORDER_COLUMNS_UF.value]

    densidade_uf = densidade_uf.rename(
        columns=anatel_constants.RENAME_COLUMNS_UF.value
    )

    densidade_uf["densidade"] = (
        densidade_uf["densidade"].astype(str).str.replace(",", ".").astype(float)
    )
    log("Salvando dados por UF")
    densidade_uf.to_csv(
        f"{anatel_constants.TABLES_OUTPUT_PATH.value[table_id]}densidade_uf.csv",
        index=False,
        sep=",",
        encoding="utf-8",
        na_rep="",
    )


# ! TASK MUNICIPIO
def clean_csv_municipio(table_id):
    log("Abrindo os dados por município...")
    densidade = pd.read_csv(
        f"{anatel_constants.INPUT_PATH.value}Densidade_Telefonia_Movel.csv",
        sep=";",
        encoding="utf-8",
    )
    densidade.rename(columns={"Nível Geográfico Densidade": "geografia"}, inplace=True)
    densidade_municipio = densidade[densidade["geografia"] == "Municipio"]
    densidade_municipio = densidade_municipio[
        anatel_constants.ORDER_COLUMNS_MUNICIPIO.value
    ]
    densidade_municipio = densidade_municipio.rename(
        columns=anatel_constants.RENAME_COLUMNS_MUNICIPIO.value
    )
    densidade_municipio["densidade"] = (
        densidade_municipio["densidade"].astype(str).str.replace(",", ".").astype(float)
    )
    log("Salvando os dados por município...")
    densidade_municipio.to_csv(
        f"{anatel_constants.TABLES_OUTPUT_PATH.value[table_id]}densidade_municipio.csv",
        index=False,
        sep=",",
        encoding="utf-8",
        na_rep="",
    )

@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def join_tables_in_function(table_id, semestre, ano):
    os.system(f"mkdir -p {anatel_constants.TABLES_OUTPUT_PATH.value[table_id]}")
    if table_id == 'microdados':
        clean_csv_microdados(ano=ano, semestre = semestre, table_id=table_id)

    elif table_id == 'densidade_brasil':
        clean_csv_brasil(table_id=table_id)

    elif table_id == 'densidade_uf':
        clean_csv_uf(table_id=table_id)

    elif table_id == 'densidade_municipio':
        clean_csv_municipio(table_id=table_id)

    return anatel_constants.TABLES_OUTPUT_PATH.value[table_id]

def get_max_date_in_table_microdados(ano: int, semestre: int):
    unzip_file()
    log("Obtendo a data máxima da tabela microdados...")
    df = pd.read_csv(
        f"{anatel_constants.INPUT_PATH.value}Acessos_Telefonia_Movel_{ano}_{semestre}S.csv",
        sep=";",
        encoding="utf-8",
        dtype=str
    )
    df['data'] = df['Ano'] + '-' + df['Mês'] + '-01'
    log(df['data'].max())
    return df['data'].max()