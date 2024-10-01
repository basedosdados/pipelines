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
    process_all_files
)

from pipelines.utils.utils import log
from pipelines.constants import constants


#wrapper para usar while e gerenciar erors
def retry_download_car(car, state, polygon, folder, max_retries=8):
    retries = 0
    success = False

    while retries < max_retries and not success:
        try:
            car.download_state(state=state, polygon=polygon, folder=folder)
            success = True
            log(f'Download do estado {state} concluído com sucesso.')
        except httpx.ReadTimeout as e:
            retries += 1
            log(f'Erro de timeout ao baixar {state}. Tentativa {retries} de {max_retries}. Exceção: {e}')
            log(f'Tentando novamente em 8 segundos')
            tm.sleep(8)
            if retries >= max_retries:
                log(f'Falha ao baixar o estado {state} após {max_retries} tentativas.')
                raise e
        except Exception as e:
            log(f'Erro inesperado ao baixar {state}: {e}')
            raise e


@task(
    max_retries=3,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def download_car(inputpath, outputpath, sigla_uf):
    os.makedirs(f'{inputpath}', exist_ok=True)
    os.makedirs(f'{outputpath}', exist_ok=True)

    log('Downloading CAR')

    car = Sicar()

    log(f'Iniciando o download do estado {sigla_uf}')

    try:
        retry_download_car(
            car=car,
            state=sigla_uf,
            polygon=Polygon.AREA_PROPERTY,
            folder=inputpath,
            max_retries=5  # Quantidade de tentativas de retry
        )
    except httpx.ReadTimeout:
        log(f'Erro final ao baixar {sigla_uf} após múltiplas tentativas.')
    except Exception as e:
        log(f'Erro geral ao baixar {sigla_uf}: {e}')


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def get_each_uf_release_date()-> Dict:

    car = Sicar()
    log('Extracting UFs relasea date')

    car = Sicar()

    ufs_release_dates = car.get_release_dates()

    return ufs_release_dates




@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def unzip_to_parquet(inputpath, outputpath,uf_relase_dates):
    process_all_files(zip_folder=inputpath, output_folder=outputpath,uf_relase_dates=uf_relase_dates)


