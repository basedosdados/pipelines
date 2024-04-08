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
    clean_csv_microdados,
    clean_csv_brasil,
    clean_csv_municipio,
    clean_csv_uf,
)
from pipelines.utils.utils import log, to_partitions


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

@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def get_max_date_in_table_microdados(ano: int, semestre: int):
    unzip_file()
    log("Obtendo a data máxima da tabela microdados...")
    df = pd.read_csv(
        f"{anatel_constants.INPUT_PATH.value}Acessos_Telefonia_Movel_{ano}_{semestre}S.csv",
        sep=";",
        encoding="utf-8",
        dtype=str
    )
    df['data'] = df['Ano'] + '-' + df['Mês']

    df['data'] = pd.to_datetime(df['data'], format="%Y-%m")

    log(df['data'].max())

    return df['data'].max()