# -*- coding: utf-8 -*-


import datetime

import polars as pl
from prefect import task

from pipelines.datasets.br_denatran_frota.constants import constants
from pipelines.datasets.br_denatran_frota.handlers import (
    crawl,
    get_desired_file,
    get_latest_data,
    get_year_month_from_filename,
    output_file_to_parquet,
    should_process_data,
    treat_municipio_tipo,
    treat_uf_tipo,
)
from pipelines.utils.utils import log

MONTHS = constants.MONTHS.value
DATASET = constants.DATASET.value
DICT_UFS = constants.DICT_UFS.value
OUTPUT_PATH = constants.OUTPUT_PATH.value


@task()  # noqa
def crawl_task(month: int, year: int, temp_dir: str = "") -> tuple:
    return crawl(month, year, temp_dir)


@task()
def treat_uf_tipo_task(file) -> pl.DataFrame:
    log(file)
    return treat_uf_tipo(file)


@task()
def output_file_to_parquet_task(df: pl.DataFrame, filename: str) -> None:
    return output_file_to_parquet(df, filename)


@task()
def get_desired_file_task(year: int, download_directory: str, filetype: str) -> str:
    return get_desired_file(year, download_directory, filetype)


@task()
def treat_municipio_tipo_task(file: str) -> pl.DataFrame:
    return treat_municipio_tipo(file)


@task()
def get_latest_data_task(table_id: str, dataset_id: str) -> tuple[int, int]:
    return get_latest_data(table_id=table_id, dataset_id=dataset_id)


@task()
def should_process_data_task(bq_year: int, bq_month: int, filename: str) -> bool:
    return should_process_data(bq_year, bq_month, filename)


@task()
def get_denatran_date(filename: str) -> datetime.date:
    year, month = get_year_month_from_filename(filename)
    return datetime.date(year, month, 1)
