# -*- coding: utf-8 -*-
"""
Tasks for br_cgu_beneficios_cidadao
"""

from datetime import timedelta

import pandas as pd
from prefect import task

from pipelines.datasets.br_cgu_beneficios_cidadao.constants import constants
from pipelines.datasets.br_cgu_beneficios_cidadao.utils import (
    download_unzip_csv,
    extract_dates,
    parquet_partition,
)
from pipelines.utils.utils import log


@task
def print_last_file(file):
    log(f"Arquivo baixado --> {file}")


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def crawl_last_date(table_id: str) -> list:
    """
    Encontra a data mais recente em um DataFrame e retorna a data e a URL correspondente.

    Parâmetros:
    - table_id (str): O identificador da tabela que contém os dados.

    Retorna:
    Uma lista contendo a data mais recente e a URL correspondente.

    Exemplo de uso:
    date, url = crawl_last_date("minha_tabela")
    """
    dates = extract_dates(table=table_id)
    dates["data"] = pd.to_datetime(
        dates["ano"].astype(str) + "-" + dates["mes_numero"].astype(str) + "-01"
    )
    max_row = dates[dates["data"] == dates["data"].max()]

    max_date = max_row["data"].iloc[0]

    return [max_date, max_row["urls"].iloc[0]]


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)  # noqa
def crawler_bolsa_familia(historical_data: bool, file, year= "2023"):
    if historical_data:
        dates = extract_dates(table="novo_bolsa_familia")
        log(dates.dtypes)
        endpoints = dates[dates["ano"] == year]["urls"].to_list()

        log("BAIXANDO DADOS HISTÓRICOS")
        log(f"ENDPOINTS >> {endpoints}")

        download_unzip_csv(
            url=constants.MAIN_URL_NOVO_BOLSA_FAMILIA.value, files=endpoints, id="novo_bolsa_familia"
        )
    else:
        log("BAIXANDO DADOS MAIS RECENTES")
        download_unzip_csv(
            url=constants.MAIN_URL_NOVO_BOLSA_FAMILIA.value,
            files=f"{file}",
            id="novo_bolsa_familia",
        )

    parquet_partition(
        path="/tmp/data/br_cgu_beneficios_cidadao/novo_bolsa_familia/input/",
        table="novo_bolsa_familia",
    )
    return "/tmp/data/br_cgu_beneficios_cidadao/novo_bolsa_familia/output/"


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)  # noqa
def crawler_garantia_safra(historical_data: bool, file, year = "2023"):
    if historical_data:
        dates = extract_dates(table="garantia_safra")

        endpoints = dates[dates["ano"] == year]["urls"].to_list()

        log("BAIXANDO DADOS HISTÓRICOS")
        log(f"ENDPOINTS >> {endpoints}")

        download_unzip_csv(
            url=constants.MAIN_URL_GARANTIA_SAFRA.value,
            files=endpoints,
            id="garantia_safra",
        )
    else:
        log("BAIXANDO DADOS MAIS RECENTES")
        download_unzip_csv(
            url=constants.MAIN_URL_GARANTIA_SAFRA.value,
            files=f"{file}",
            id="garantia_safra",
        )

    parquet_partition(
        path="/tmp/data/br_cgu_beneficios_cidadao/garantia_safra/input/",
        table="garantia_safra",
    )
    return "/tmp/data/br_cgu_beneficios_cidadao/garantia_safra/output/"


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)  # noqa
def crawler_bpc(historical_data: bool, file, year = "2023"):
    if historical_data:
        dates = extract_dates(table="bpc")

        endpoints = dates[dates["ano"] == year]["urls"].to_list()

        log("BAIXANDO DADOS HISTÓRICOS")
        log(f"ENDPOINTS >> {endpoints}")

        download_unzip_csv(
            url=constants.MAIN_URL_BPC.value,
            files=endpoints,
            id="bpc",
        )
    else:
        log("BAIXANDO DADOS MAIS RECENTES")
        download_unzip_csv(
            url=constants.MAIN_URL_BPC.value,
            files=f"{file}",
            id="bpc",
        )

    parquet_partition(
        path="/tmp/data/br_cgu_beneficios_cidadao/bpc/input/",
        table="bpc",
    )
    return "/tmp/data/br_cgu_beneficios_cidadao/bpc/output/"
