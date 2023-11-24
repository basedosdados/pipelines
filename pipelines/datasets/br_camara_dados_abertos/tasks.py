# -*- coding: utf-8 -*-
from prefect import task

from pipelines.datasets.br_camara_dados_abertos.constants import constants
from pipelines.datasets.br_camara_dados_abertos.utils import (
    get_date,
    read_and_clean_camara_dados_abertos,
)
from pipelines.utils.utils import log, to_partitions


# ! Microdados
@task
def make_partitions(table_id, date_column) -> str:
    df = read_and_clean_camara_dados_abertos(
        path=constants.INPUT_PATH.value,
        table_id=f"{table_id}",
        date_column=date_column,
    )
    log(f"particionando {table_id}")
    log(df.columns)
    to_partitions(
        data=df,
        partition_columns=["ano"],
        savepath=f"{constants.OUTPUT_PATH.value}/{table_id}/",
    )

    return f"{constants.OUTPUT_PATH.value}/{table_id}/"


# ! Obtendo a data máxima.
@task
def get_date_max():
    df = get_date()
    data_max = df["data"].max()

    return data_max
