# -*- coding: utf-8 -*-
"""
Tasks for br_sfb_sicar
"""



from prefect import task
from SICAR import Sicar, Polygon, State
import os
from datetime import datetime, timedelta

from pipelines.datasets.br_sfb_sicar.constants import (
    Constants as car_constants,
)

from pipelines.datasets.br_sfb_sicar.utils import (
    process_all_files
)

from pipelines.utils.utils import log
from pipelines.constants import constants



inputpath = car_constants.INPUT_PATH.value
outputpath = car_constants.OUTPUT_PATH.value


#? 1. check for updates
#SIcar package has a method _parse_release_dates, which  returns a dcit with states and release dates;
#! diferent states have different updates | not supported by basedosdados currently metadada registry
# one table per state no | need several conjuntos cause there nine


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def download_car(inputpath, outputpath):

    os.makedirs( f'{inputpath}',exist_ok=True)
    os.makedirs( f'{outputpath}',exist_ok=True)
    ufs = ['AC', 'PA', 'MT', 'PB', 'RR', 'RO']
    log('Downloading Car')
    car = Sicar()
    #TODo: make async cals
    for uf in ufs:
        log(f'downloading state {uf}')
        car.download_state(
            state=uf,
            polygon=Polygon.AREA_PROPERTY,
            folder=inputpath)


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def unzip_to_parquet(inputpath, outputpath):
    process_all_files(zip_folder=inputpath, output_folder=outputpath)


