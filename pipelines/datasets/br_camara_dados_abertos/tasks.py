# -*- coding: utf-8 -*-
# register tasks
import os
from datetime import timedelta

import pandas as pd
from prefect import task

from pipelines.constants import constants
from pipelines.datasets.br_camara_dados_abertos.constants import (
    constants as constants_camara,
)
from pipelines.datasets.br_camara_dados_abertos.utils import (
    download_and_read_data,
    get_data,
    get_data_deputados,
    read_and_clean_camara_dados_abertos,
    read_and_clean_data_deputados,
)
from pipelines.utils.utils import log, to_partitions


# ! Microdados
@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
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
@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def download_files_and_get_max_date():
    df = get_data()
    data_max = df["data"].max()

    return data_max


# -------------------------------------------------------------------> Deputados
@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
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


# ----------------------------------------> DADOS CAMARA ABERTA - UNIVERSAL


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def save_data(table_id: str):
    if not os.path.exists(f"{constants_camara.OUTPUT_PATH.value}{table_id}"):
        os.makedirs(f"{constants_camara.OUTPUT_PATH.value}{table_id}")

    constants_camara.TABLE_LIST_CAMARA.value[table_id]

    df = download_and_read_data(table_id)
    output_path = constants_camara.TABLES_OUTPUT_PATH.value[table_id]

    if table_id == "proposicao_microdados":
        output_path = constants_camara.TABLES_OUTPUT_PATH.value[table_id]
        df["ultimoStatus_despacho"] = df["ultimoStatus_despacho"].apply(
            lambda x: str(x).replace(";", ",").replace("\n", " ").replace("\r", " ")
        )
        df["ementa"] = df["ementa"].apply(
            lambda x: str(x).replace(";", ",").replace("\n", " ").replace("\r", " ")
        )
        df["ano"] = df.apply(
            lambda x: x["dataApresentacao"][0:4] if x["ano"] == 0 else x["ano"],
            axis=1,
        )

    if table_id == "frente_deputado":
        output_path = constants_camara.TABLES_OUTPUT_PATH.value[table_id]
        df = df.rename(columns=constants_camara.RENAME_COLUMNS_FRENTE_DEPUTADO.value)

    if table_id == "evento":
        output_path = constants_camara.TABLES_OUTPUT_PATH.value[table_id]
        df = df.rename(columns=constants_camara.RENAME_COLUMNS_EVENTO.value)
        df["descricao"] = df["descricao"].apply(
            lambda x: str(x).replace("\n", " ").replace("\r", " ")
        )

    df.to_csv(output_path, sep=",", index=False, encoding="utf-8")


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def output_path_list(table_id_list):
    output_path_list = []
    for table_id in table_id_list:
        output_path_list.append(f"{constants_camara.OUTPUT_PATH.value}{table_id}/")
    return output_path_list

@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def dict_list_parameters(dataset_id, materialization_mode, dbt_alias):
    table_id = [
        "proposicao_microdados",
        "proposicao_autor",
        "proposicao_tema",
        "orgao",
        "orgao_deputado",
        "evento",
        "evento_orgao",
        "evento_presenca_deputado",
        "evento_requerimento",
        "frente",
        "frente_deputado",
        "funcionario",
    ]

    parameters = [
        dict(
            dataset_id=dataset_id,
            table_id=table_id[0],
            mode=materialization_mode,
            dbt_alias=dbt_alias,
            dbt_command="run",
        ),
        dict(
            dataset_id=dataset_id,
            table_id=table_id[1],
            mode=materialization_mode,
            dbt_alias=dbt_alias,
            dbt_command="run",
        ),
        dict(
            dataset_id=dataset_id,
            table_id=table_id[2],
            mode=materialization_mode,
            dbt_alias=dbt_alias,
            dbt_command="run",
        ),
        dict(
            dataset_id=dataset_id,
            table_id=table_id[3],
            mode=materialization_mode,
            dbt_alias=dbt_alias,
            dbt_command="run",
        ),
        dict(
            dataset_id=dataset_id,
            table_id=table_id[4],
            mode=materialization_mode,
            dbt_alias=dbt_alias,
            dbt_command="run",
        ),
        dict(
            dataset_id=dataset_id,
            table_id=table_id[5],
            mode=materialization_mode,
            dbt_alias=dbt_alias,
            dbt_command="run",
        ),
        dict(
            dataset_id=dataset_id,
            table_id=table_id[6],
            mode=materialization_mode,
            dbt_alias=dbt_alias,
            dbt_command="run",
        ),
        dict(
            dataset_id=dataset_id,
            table_id=table_id[7],
            mode=materialization_mode,
            dbt_alias=dbt_alias,
            dbt_command="run",
        ),
        dict(
            dataset_id=dataset_id,
            table_id=table_id[8],
            mode=materialization_mode,
            dbt_alias=dbt_alias,
            dbt_command="run",
        ),
        dict(
            dataset_id=dataset_id,
            table_id=table_id[9],
            mode=materialization_mode,
            dbt_alias=dbt_alias,
            dbt_command="run",
        ),
        dict(
            dataset_id=dataset_id,
            table_id=table_id[10],
            mode=materialization_mode,
            dbt_alias=dbt_alias,
            dbt_command="run",
        ),
        dict(
            dataset_id=dataset_id,
            table_id=table_id[11],
            mode=materialization_mode,
            dbt_alias=dbt_alias,
            dbt_command="run",
        ),
    ]
    return parameters
