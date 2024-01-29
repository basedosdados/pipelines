# -*- coding: utf-8 -*-
import os
from datetime import timedelta

import pandas as pd
import requests
from prefect import task

from pipelines.constants import constants
from pipelines.datasets.br_camara_dados_abertos.constants import (
    constants as constants_camara,
)
from pipelines.datasets.br_camara_dados_abertos.utils import (
    download_and_read_data_proposicao,
    get_data,
    get_data_deputados,
    read_and_clean_camara_dados_abertos,
    read_and_clean_data_deputados,
)
from pipelines.utils.utils import log, to_partitions


# ! Microdados
@task
def make_partitions(table_id, date_column) -> str:
    """
    Make partitions for a given table based on a date column.

    Args:
        table_id (str): The ID of the table.
        date_column (str): The name of the date column.

    Returns:
        str: The path where the partitions are saved.
    """
    df = read_and_clean_camara_dados_abertos(
        path=constants_camara.INPUT_PATH.value,
        table_id=f"{table_id}",
        date_column=date_column,
    )
    log(f"particionando {table_id}")
    to_partitions(
        data=df,
        partition_columns=["ano"],
        savepath=f"{constants_camara.OUTPUT_PATH.value}/{table_id}/",
    )

    return f"{constants_camara.OUTPUT_PATH.value}/{table_id}/"


# ! Obtendo a data máxima.
@task
def download_files_and_get_max_date():
    df = get_data()
    data_max = df["data"].max()

    return data_max


# -------------------------------------------------------------------> Deputados
@task
def download_files_and_get_max_date_deputados():
    df = get_data_deputados()

    df["dataHora"] = pd.to_datetime(df["dataHora"])

    data_max = df["dataHora"].max()

    return data_max


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def treat_and_save_table(table_id):
    log(f"------------- TRATANDO {table_id} --------------")
    df = read_and_clean_data_deputados(table_id=table_id)

    if not os.path.exists(f"{constants_camara.OUTPUT_PATH.value}{table_id}"):
        os.makedirs(f"{constants_camara.OUTPUT_PATH.value}{table_id}")

    log(f"Saving {table_id} to {constants_camara.OUTPUT_PATH.value}{table_id}/data.csv")

    df.to_csv(
        f"{constants_camara.OUTPUT_PATH.value}{table_id}/data.csv", sep=",", index=False
    )

    log(f"{constants_camara.OUTPUT_PATH.value}{table_id}/data.csv")

    return f"{constants_camara.OUTPUT_PATH.value}{table_id}/data.csv"


# -------------------------------------------------------------------> PROPOSIÇÃO


@task
def save_data_proposicao(table_id: str):
    df = download_and_read_data_proposicao(table_id)
    valor = constants_camara.TABLE_LIST_CAMARA.value[table_id]
    if not os.path.exists(f"{constants_camara.OUTPUT_PATH.value}{table_id}"):
        os.makedirs(f"{constants_camara.OUTPUT_PATH.value}{table_id}")

    if table_id == "proposicao_microdados":
        valor = constants_camara.TABLE_LIST_CAMARA.value[table_id]
        url = f"http://dadosabertos.camara.leg.br/arquivos/{valor}/csv/{valor}-{constants_camara.ANOS_ATUAL.value}.csv"
        response = requests.get(url)
        if response.status_code == 200:
            df["ultimoStatus_despacho"] = df["ultimoStatus_despacho"].apply(
                lambda x: str(x).replace(";", ",").replace("\n", "").replace("\r", "")
            )
            df["ementa"] = df["ementa"].apply(
                lambda x: str(x).replace(";", ",").replace("\n", "").replace("\r", "")
            )
            df["ano"] = df.apply(
                lambda x: x["dataApresentacao"][0:4] if x["ano"] == 0 else x["ano"],
                axis=1,
            )
            df.to_csv(
                f"{constants_camara.OUTPUT_PATH.value}{table_id}/{valor}_{constants_camara.ANOS_ATUAL.value}.csv",
                sep=",",
                index=False,
                encoding="utf-8",
            )
        elif response.status_code >= 400 and response.status_code <= 599:
            url_2 = f"http://dadosabertos.camara.leg.br/arquivos/{valor}/csv/{valor}-{constants_camara.ANOS.value}.csv"
            response = requests.get(url_2)

            df["ultimoStatus_despacho"] = df["ultimoStatus_despacho"].apply(
                lambda x: str(x).replace(";", ",").replace("\n", "").replace("\r", "")
            )
            df["ementa"] = df["ementa"].apply(
                lambda x: str(x).replace(";", ",").replace("\n", "").replace("\r", "")
            )
            df["ano"] = df.apply(
                lambda x: x["dataApresentacao"][0:4] if x["ano"] == 0 else x["ano"],
                axis=1,
            )
            df.to_csv(
                f"{constants_camara.OUTPUT_PATH.value}{table_id}/{valor}_{constants_camara.ANOS.value}.csv",
                sep=",",
                index=False,
                encoding="utf-8",
            )

    elif table_id in ["proposicao_autor", "proposicao_tema"]:
        valor = constants_camara.TABLE_LIST_CAMARA.value[table_id]
        url = f"http://dadosabertos.camara.leg.br/arquivos/{valor}/csv/{valor}-{constants_camara.ANOS_ATUAL.value}.csv"
        response = requests.get(url)
        if response.status_code == 200:
            df.to_csv(
                f"{constants_camara.OUTPUT_PATH.value}{table_id}/{valor}_{constants_camara.ANOS_ATUAL.value}.csv",
                sep=",",
                index=False,
                encoding="utf-8",
            )
        elif response.status_code >= 400 and response.status_code <= 599:
            df.to_csv(
                f"{constants_camara.OUTPUT_PATH.value}{table_id}/{valor}_{constants_camara.ANOS.value}.csv",
                sep=",",
                index=False,
                encoding="utf-8",
            )

    elif table_id == "orgao":
        df = pd.read_csv(f"{constants_camara.INPUT_PATH.value}{valor}.csv", sep=";")
        df.to_csv(
            f"{constants_camara.OUTPUT_PATH.value}{table_id}/{valor}.csv",
            sep=",",
            index=False,
            encoding="utf-8",
        )

    elif table_id == "orgao_deputado":
        df = pd.read_csv(f"{constants_camara.INPUT_PATH.value}{valor}.csv", sep=";")
        df.to_csv(
            f"{constants_camara.OUTPUT_PATH.value}{table_id}/{valor}-L57.csv",
            sep=",",
            index=False,
            encoding="utf-8",
        )


@task
def output_path_list(table_id_list):
    output_path_list = []
    for table_id in table_id_list:
        output_path_list.append(f"{constants_camara.OUTPUT_PATH.value}{table_id}/")
    return output_path_list


@task
def dict_list_parameters(dataset_id, materialization_mode, dbt_alias):
    table_id = [
        "proposicao_microdados",
        "proposicao_autor",
        "proposicao_tema",
        "orgao",
        "orgao_deputado",
    ]

    parameters = [
        dict(
            dataset_id=dataset_id,
            table_id=table_id[0],
            mode=materialization_mode,
            dbt_alias=dbt_alias,
            dbt_command="run and test",
        ),
        dict(
            dataset_id=dataset_id,
            table_id=table_id[1],
            mode=materialization_mode,
            dbt_alias=dbt_alias,
            dbt_command="run and test",
        ),
        dict(
            dataset_id=dataset_id,
            table_id=table_id[2],
            mode=materialization_mode,
            dbt_alias=dbt_alias,
            dbt_command="run and test",
        ),
        dict(
            dataset_id=dataset_id,
            table_id=table_id[3],
            mode=materialization_mode,
            dbt_alias=dbt_alias,
            dbt_command="run and test",
        ),
        dict(
            dataset_id=dataset_id,
            table_id=table_id[4],
            mode=materialization_mode,
            dbt_alias=dbt_alias,
            dbt_command="run and test",
        ),
    ]
    return parameters
