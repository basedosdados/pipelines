# -*- coding: utf-8 -*-
"""
Tasks for br_b3_cotacoes
"""

from prefect import task
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pipelines.datasets.br_b3_cotacoes.constants import (
    constants as br_b3_cotacoes_constants,
)

from pipelines.utils.utils import (
    log,
)
from pipelines.constants import constants

from pipelines.datasets.br_b3_cotacoes.utils import (
    download_unzip_csv,
    read_files,
    partition_data,
)


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def tratamento(delta_day: int):
    """
    Retrieve b3 quotes data for a specific date and create the path to download and open the file.

    Args:

    days_to_run (int): The number of days for which I want to retrieve data from b3.

    Returns:

        str: the file path to download and open b3 files.
    """

    day = (datetime.now() - timedelta(days=delta_day)).strftime("%d-%m-%Y")

    day_url = datetime.strptime(day, "%d-%m-%Y").strftime("%Y-%m-%d")

    download_unzip_csv(
        url=br_b3_cotacoes_constants.B3_URL.value.format(day_url),
        path=br_b3_cotacoes_constants.B3_PATH_INPUT.value,
    )
    log(
        "********************************ABRINDO O ARQUIVO********************************"
    )

    read_files(br_b3_cotacoes_constants.B3_PATH_INPUT_TXT.value.format(day))


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def make_partition(df):
    partition_data(
        df,
        column_name="data_referencia",
        output_directory=br_b3_cotacoes_constants.B3_PATH_OUTPUT.value,
    )
    return br_b3_cotacoes_constants.B3_PATH_OUTPUT.value


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def data_max_b3(df):
    max_value = pd.to_datetime(df["data_referencia"]).max()
    return max_value.strftime("%Y-%m-%d")
