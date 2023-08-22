# -*- coding: utf-8 -*-
"""
Tasks for br_b3_cotacoes
"""

from prefect import task
import os
import pandas as pd
from datetime import datetime, timedelta
from pipelines.datasets.br_b3_cotacoes.constants import (
    constants as br_b3_cotacoes_constants,
)
from pipelines.utils.utils import log
from pipelines.constants import constants
from pipelines.datasets.br_b3_cotacoes.utils import (
    download_chunk_and_unzip_csv,
    process_chunk_csv,
)


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def tratamento(delta_day: int):
    day = (datetime.now() - timedelta(days=delta_day)).strftime("%d-%m-%Y")
    day_url = datetime.strptime(day, "%d-%m-%Y").strftime("%Y-%m-%d")
    download_chunk_and_unzip_csv(
        url=br_b3_cotacoes_constants.B3_URL.value.format(day_url),
        path=br_b3_cotacoes_constants.B3_PATH_INPUT.value,
    )
    process_chunk_csv(br_b3_cotacoes_constants.B3_PATH_INPUT_TXT.value.format(day))

    log(br_b3_cotacoes_constants.B3_PATH_OUTPUT.value)
    return br_b3_cotacoes_constants.B3_PATH_OUTPUT.value


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def data_max_b3(delta_day: int):
    day = (datetime.now() - timedelta(days=delta_day)).strftime("%d-%m-%Y")
    df = pd.read_csv(br_b3_cotacoes_constants.B3_PATH_INPUT_TXT.value.format(day))
    log(df.columns)
    log(df["DataReferencia"].unique())
    max_value = pd.to_datetime(df["DataReferencia"]).max()
    return max_value.strftime("%Y-%m-%d")
