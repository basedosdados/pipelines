# -*- coding: utf-8 -*-

from prefect import task
import glob
import os
from pipelines.datasets.br_denatran_frota.constants import constants
import pandas as pd
import polars as pl
from pipelines.datasets.br_denatran_frota.handlers import (
    crawl,
    treat_uf_tipo,
    output_file_to_csv,
    get_desired_file,
    treat_municipio_tipo,
    get_latest_data,
)
from pipelines.utils.utils import (
    log,
)

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
def output_file_to_csv_task(df: pl.DataFrame, filename: str) -> None:
    return output_file_to_csv(df, filename)


@task()
def get_desired_file_task(year: int, download_directory: str, filetype: str) -> str:
    return get_desired_file(year, download_directory, filetype)


@task()
def treat_municipio_tipo_task(file: str) -> pl.DataFrame:
    return treat_municipio_tipo(file)


@task()
def get_latest_data_task(table_name: str) -> tuple[int, int]:
    return get_latest_data(table_name)
