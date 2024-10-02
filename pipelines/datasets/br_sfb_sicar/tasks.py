# -*- coding: utf-8 -*-
"""
Tasks for br_sfb_sicar
"""

from prefect import task
from SICAR import Sicar, Polygon, State
import os
from datetime import datetime, timedelta
from typing import Dict
import httpx
import time as tm
from pipelines.datasets.br_sfb_sicar.constants import (
    Constants as car_constants,
)

from pipelines.datasets.br_sfb_sicar.utils import (
    process_all_files,
    retry_download_car,
)

from pipelines.utils.utils import log
from pipelines.constants import constants


@task(
    max_retries=3,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def download_car(inputpath, outputpath, sigla_uf, polygon):
    os.makedirs(f'{inputpath}', exist_ok=True)
    os.makedirs(f'{outputpath}', exist_ok=True)

    log('Baixando o CAR')

    car = Sicar()

    log(f'Iniciando o download do estado {sigla_uf}')

    try:
        retry_download_car(
            car=car,
            state=sigla_uf,
            polygon=polygon,
            folder=inputpath,
            max_retries=5
        )
    except httpx.ReadTimeout as e:
        log(f'Erro de Timeout {e} ao baixar dados de {sigla_uf} após múltiplas tentativas.')
    except Exception as e:
        log(f'Erro geral ao baixar {sigla_uf}: {e}')


@task(
    max_retries=10,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def get_each_uf_release_date()-> Dict:

    car = Sicar()
    log('Extraindo a data de atualização dos dados de cada UF')

    car = Sicar()

    ufs_release_dates = car.get_release_dates()

    return ufs_release_dates




@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def unzip_to_parquet(inputpath, outputpath,uf_relase_dates):
    process_all_files(zip_folder=inputpath, output_folder=outputpath,uf_relase_dates=uf_relase_dates)


