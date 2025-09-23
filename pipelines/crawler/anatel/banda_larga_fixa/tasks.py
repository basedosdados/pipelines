# -*- coding: utf-8 -*-
import os
from datetime import timedelta

import pandas as pd
from prefect import task

from pipelines.constants import constants
from pipelines.crawler.anatel.banda_larga_fixa.constants import (
    constants as anatel_constants,
)
from pipelines.crawler.anatel.banda_larga_fixa.utils import (
    get_year,
    treatment,
    treatment_br,
    treatment_municipio,
    treatment_uf,
    unzip_file,
)
from pipelines.utils.utils import log


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def join_tables_in_function(table_id: str, ano):
    os.system(
        f"mkdir -p {anatel_constants.TABLES_OUTPUT_PATH.value[table_id]}"
    )
    if table_id == "microdados":
        treatment(ano=ano, table_id=table_id)

    elif table_id == "densidade_brasil":
        treatment_br(table_id=table_id)

    elif table_id == "densidade_uf":
        treatment_uf(table_id=table_id)

    elif table_id == "densidade_municipio":
        treatment_municipio(table_id=table_id)

    return anatel_constants.TABLES_OUTPUT_PATH.value[table_id]


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def get_max_date_in_table_microdados(table_id: str, ano: int):
    if table_id == "microdados":
        log(
            f"{anatel_constants.INPUT_PATH.value}Acessos_Banda_Larga_Fixa_{ano}.csv"
        )
        df = pd.read_csv(
            f"{anatel_constants.INPUT_PATH.value}Acessos_Banda_Larga_Fixa_{ano}.csv",
            sep=";",
            encoding="utf-8",
            dtype=str,
        )
        df["data"] = df["Ano"] + "-" + df["Mês"]

        df["data"] = pd.to_datetime(df["data"], format="%Y-%m")

        return df["data"].max()

    else:
        log(
            f"{anatel_constants.INPUT_PATH.value}Densidade_Banda_Larga_Fixa.csv"
        )

        df = pd.read_csv(
            f"{anatel_constants.INPUT_PATH.value}Densidade_Banda_Larga_Fixa.csv",
            sep=";",
            encoding="utf-8",
            dtype=str,
        )
        df["data"] = df["Ano"] + "-" + df["Mês"]

        df["data"] = pd.to_datetime(df["data"], format="%Y-%m")

        return df["data"].max()


@task
def get_year_and_unzip(day):
    if day is None:
        unzip_file()

    return get_year()
