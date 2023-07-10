# -*- coding: utf-8 -*-
"""
Tasks for br_denatran_frota
"""

###############################################################################
#
# Aqui é onde devem ser definidas as tasks para os flows do projeto.
# Cada task representa um passo da pipeline. Não é estritamente necessário
# tratar todas as exceções que podem ocorrer durante a execução de uma task,
# mas é recomendável, ainda que não vá implicar em  uma quebra no sistema.
# Mais informações sobre tasks podem ser encontradas na documentação do
# Prefect: https://docs.prefect.io/core/concepts/tasks.html
#
# De modo a manter consistência na codebase, todo o código escrito passará
# pelo pylint. Todos os warnings e erros devem ser corrigidos.
#
# As tasks devem ser definidas como funções comuns ao Python, com o decorador
# @task acima. É recomendado inserir type hints para as variáveis.
#
# Um exemplo de task é o seguinte:
#
# -----------------------------------------------------------------------------
# from prefect import task
#
# @task
# def my_task(param1: str, param2: int) -> str:
#     """
#     My task description.
#     """
#     return f'{param1} {param2}'
# -----------------------------------------------------------------------------
#
# Você também pode usar pacotes Python arbitrários, como numpy, pandas, etc.
#
# -----------------------------------------------------------------------------
# from prefect import task
# import numpy as np
#
# @task
# def my_task(a: np.ndarray, b: np.ndarray) -> str:
#     """
#     My task description.
#     """
#     return np.add(a, b)
# -----------------------------------------------------------------------------
#
# Abaixo segue um código para exemplificação, que pode ser removido.
#
###############################################################################

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
    get_latest_data
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
    log(filename)
    return output_file_to_csv(df, filename)


@task()
def get_desired_file_task(year: int, download_directory: str, filetype: str) -> str:
    return get_desired_file(year, download_directory, filetype)


@task()
def treat_municipio_tipo_task(file: str) -> pl.DataFrame:
    return treat_municipio_tipo(file)

@task()
def get_latest_data_task(table_name: str):
    return get_latest_data(table_name)
