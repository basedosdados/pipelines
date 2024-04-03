# -*- coding: utf-8 -*-
import os
from datetime import timedelta
import numpy as np
import pandas as pd
from prefect import task
from pipelines.constants import constants
from pipelines.utils.crawler_anatel.banda_larga_fixa.constants import (
    constants as anatel_constants,
)
from pipelines.utils.crawler_anatel.banda_larga_fixa.utils import (
    check_and_create_column,
    unzip_file
)
from pipelines.utils.utils import log, to_partitions

def treatment(table_id:str, ano: int):
    log("Iniciando o tratamento do arquivo microdados da Anatel")
    df = pd.read_csv(
        f"{anatel_constants.INPUT_PATH.value}Acessos_Banda_Larga_Fixa_{ano}.csv",
        sep=";",
        encoding="utf-8",
    )
    df = check_and_create_column(df, "Tipo de Produto")
    df.rename(
        columns=anatel_constants.RENAME_MICRODADOS.value,
        inplace=True,
    )
    df.drop(anatel_constants.DROP_COLUMNS_MICRODADOS.value, axis=1, inplace=True)
    df = df[
        anatel_constants.ORDER_COLUMNS_MICRODADOS.value
    ]
    df.sort_values(
        anatel_constants.SORT_VALUES_MICRODADOS.value,
        inplace=True,
    )
    df.replace(np.nan, "", inplace=True)
    df["transmissao"] = df["transmissao"].apply(
        lambda x: x.replace("Cabo Metálico", "Cabo Metalico")
        .replace("Satélite", "Satelite")
        .replace("Híbrido", "Hibrido")
        .replace("Fibra Óptica", "Fibra Optica")
        .replace("Rádio", "Radio")
    )
    df["acessos"] = df["acessos"].apply(lambda x: str(x).replace(".0", ""))
    df["produto"] = df["produto"].apply(
        lambda x: x.replace("LINHA_DEDICADA", "linha dedicada").lower()
    )
    log("Salvando o arquivo microdados da Anatel")
    to_partitions(
        df,
        partition_columns=["ano", "mes", "sigla_uf"],
        savepath=anatel_constants.TABLES_OUTPUT_PATH.value[table_id],
    )


def treatment_br(table_id:str):
    log("Iniciando o tratamento do arquivo densidade brasil da Anatel")
    df = pd.read_csv(
        f"{anatel_constants.INPUT_PATH.value}Densidade_Banda_Larga_Fixa.csv",
        sep=";",
        encoding="utf-8",
    )
    df.rename(columns={"Nível Geográfico Densidade": "Geografia"}, inplace=True)
    df_brasil = df[df["Geografia"] == "Brasil"]
    df_brasil = df_brasil.drop(anatel_constants.DROP_COLUMNS_BRASIL.value, axis=1)
    df_brasil["Densidade"] = df_brasil["Densidade"].apply(
        lambda x: float(x.replace(",", "."))
    )
    df_brasil.rename(
        columns=anatel_constants.RENAME_COLUMNS_BRASIL.value,
        inplace=True,
    )
    log('Salvando o arquivo densidade brasil da Anatel')
    df_brasil.to_csv(
        f"{anatel_constants.TABLES_OUTPUT_PATH.value[table_id]}densidade_brasil.csv",
        index=False,
        sep=",",
        encoding="utf-8",
        na_rep="",
    )


def treatment_uf(table_id:str):

    log("Iniciando o tratamento do arquivo densidade uf da Anatel")
    df = pd.read_csv(
        f"{anatel_constants.INPUT_PATH.value}Densidade_Banda_Larga_Fixa.csv",
        sep=";",
        encoding="utf-8",
    )
    df.rename(columns={"Nível Geográfico Densidade": "Geografia"}, inplace=True)
    df_uf = df[df["Geografia"] == "UF"]
    df_uf.drop(anatel_constants.DROP_COLUMNS_UF.value, axis=1, inplace=True)
    df_uf["Densidade"] = df_uf["Densidade"].apply(lambda x: float(x.replace(",", ".")))
    df_uf.rename(
        columns=anatel_constants.RENAME_COLUMNS_UF.value,
        inplace=True,
    )
    log("Iniciando o particionado do arquivo densidade uf da Anatel")
    df_uf.to_csv(
        f"{anatel_constants.TABLES_OUTPUT_PATH.value[table_id]}densidade_uf.csv",
        index=False,
        sep=",",
        encoding="utf-8",
        na_rep="",
    )



def treatment_municipio(table_id:str):
    log("Iniciando o tratamento do arquivo densidade municipio da Anatel")
    df = pd.read_csv(
        f"{anatel_constants.INPUT_PATH.value}Densidade_Banda_Larga_Fixa.csv",
        sep=";",
        encoding="utf-8",
    )
    df.rename(columns={"Nível Geográfico Densidade": "Geografia"}, inplace=True)
    df_municipio = df[df["Geografia"] == "Municipio"]
    df_municipio.drop(["Município", "Geografia"], axis=1, inplace=True)
    df_municipio["Densidade"] = df_municipio["Densidade"].apply(
        lambda x: float(x.replace(",", "."))
    )
    df_municipio.rename(
        columns=anatel_constants.RENAME_COLUMNS_MUNICIPIO.value,
        inplace=True,
    )
    log("Salvando o arquivo densidade municipio da Anatel")
    to_partitions(
        df_municipio,
        partition_columns=["ano"],
        savepath=anatel_constants.TABLES_OUTPUT_PATH.value[table_id],
    )

@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def join_tables_in_function(table_id: str, ano):
    os.system(f"mkdir -p {anatel_constants.TABLES_OUTPUT_PATH.value[table_id]}")
    if table_id == 'microdados':
        treatment(ano=ano, table_id=table_id)

    elif table_id == 'densidade_brasil':
        treatment_br(table_id=table_id)

    elif table_id == 'densidade_uf':
        treatment_uf(table_id=table_id)

    elif table_id == 'densidade_municipio':
        treatment_municipio(table_id=table_id)

    return anatel_constants.TABLES_OUTPUT_PATH.value[table_id]

@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def get_max_date_in_table_microdados(ano: int):
    unzip_file()
    log("Obtendo a data máxima do arquivo microdados da Anatel")
    df = pd.read_csv(
        f"{anatel_constants.INPUT_PATH.value}Acessos_Banda_Larga_Fixa_{ano}.csv",
        sep=";",
        encoding="utf-8",
        dtype=str
    )
    df['data'] = df['Ano'] + '-' + df['Mês'] + '-01'
    log(df['data'].max())
    return df['data'].max()