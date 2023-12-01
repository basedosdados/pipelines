# -*- coding: utf-8 -*-
import pandas as pd
from prefect import task

from pipelines.datasets.br_camara_dados_abertos.constants import constants
from pipelines.datasets.br_camara_dados_abertos.utils import (
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
        path=constants.INPUT_PATH.value,
        table_id=f"{table_id}",
        date_column=date_column,
    )
    log(f"particionando {table_id}")
    to_partitions(
        data=df,
        partition_columns=["ano"],
        savepath=f"{constants.OUTPUT_PATH.value}/{table_id}/",
    )

    return f"{constants.OUTPUT_PATH.value}/{table_id}/"


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

    data_max = df["dataHora"]

    return data_max


@task
def save_table_id(table_id):
    df = read_and_clean_data_deputados()
    df.to_csv(f"{constants.OUTPUT_PATH.value}{table_id}/data.csv", sep=";}")

    return f"{constants.OUTPUT_PATH.value}{table_id}"
