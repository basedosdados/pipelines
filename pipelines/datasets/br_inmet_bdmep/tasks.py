"""
Tasks for br_inmet_bdmep
"""

from datetime import timedelta
from glob import glob

import pandas as pd
from prefect import task

from pipelines.constants import constants
from pipelines.datasets.br_inmet_bdmep.constants import ConstantsMicrodados
from pipelines.datasets.br_inmet_bdmep.utils import (
    download_inmet,
    get_clima_info,
    get_date_from_path,
    get_latest_dowload_link,
)


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def extract_last_date_from_source():
    """
    Extrai última data de atualização dos dados históricos do site do INMET.
    """

    latest_dowload_link = get_latest_dowload_link()

    download_inmet(latest_dowload_link)

    paths = glob(ConstantsMicrodados.PATH_REGEX.value)
    datas = [get_date_from_path(path) for path in paths]

    return max(datas).date()


@task
def get_base_inmet() -> str:
    """
    Faz o download dos dados meteorológicos do INMET, processa-os e salva os dataframes resultantes em arquivos CSV.

    Retorna:
    - str: o caminho para o diretório que contém os arquivos CSV de saída.
    """

    files = glob(ConstantsMicrodados.PATH_REGEX.value)

    base = pd.concat(
        [get_clima_info(file) for file in files], ignore_index=True
    )

    # ordena as colunas
    ordem = ConstantsMicrodados.COLUMNS_ORDER.value

    base = base[ordem]

    year = base.data.max().year

    # Salva o dataframe resultante em um arquivo CSV
    path_output = ConstantsMicrodados.PATH_OUTPUT.value / f"ano={year}"

    path_output.mkdir(parents=True, exist_ok=True)

    file_path = path_output / f"microdados_{year}.csv"

    base.to_csv(file_path, index=False)

    return ConstantsMicrodados.PATH_OUTPUT.value
