"""
Tasks for br_sfb_sicar
"""

import os

import httpx
from prefect import task
from SICAR import Sicar

from pipelines.constants import constants
from pipelines.crawler.sfb_sicar.utils import (
    process_all_files,
    retry_download_car,
)
from pipelines.utils.utils import log


@task(
    retries=3,
    retry_delay_seconds=constants.TASK_RETRY_DELAY.value,
)
def download_car(inputpath, outputpath, sigla_uf, polygon):
    os.makedirs(f"{inputpath}", exist_ok=True)
    os.makedirs(f"{outputpath}", exist_ok=True)

    log("Baixando o CAR")

    car = Sicar()

    log(f"Iniciando o download do estado {sigla_uf}")

    try:
        retry_download_car(
            car=car,
            state=sigla_uf,
            polygon=polygon,
            folder=inputpath,
            max_retries=8,
        )
    except httpx.ReadTimeout as e:
        log(
            f"Erro de Timeout {e} ao baixar dados de {sigla_uf} após múltiplas tentativas."
        )
        raise e
    except Exception as e:
        log(f"Erro geral ao baixar {sigla_uf}: {e}")
        raise e


@task(
    retries=10,
    retry_delay_seconds=constants.TASK_RETRY_DELAY.value,
)
def get_each_uf_release_date() -> dict:
    car = Sicar()
    log("Extraindo a data de atualização dos dados de cada UF")

    car = Sicar()

    ufs_release_dates = car.get_release_dates()

    return ufs_release_dates


@task(
    retries=constants.TASK_MAX_RETRIES.value,
    retry_delay_seconds=constants.TASK_RETRY_DELAY.value,
)
def unzip_to_parquet(inputpath, outputpath, uf_relase_dates):
    process_all_files(
        zip_folder=inputpath,
        output_folder=outputpath,
        uf_relase_dates=uf_relase_dates,
    )
