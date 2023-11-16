# -*- coding: utf-8 -*-
"""
Tasks for br_cgu_beneficios_cidadao
"""

from datetime import datetime, timedelta

import pandas as pd
import requests
from prefect import task

from pipelines.constants import constants as main_constants
from pipelines.datasets.br_cgu_beneficios_cidadao.constants import constants
from pipelines.datasets.br_cgu_beneficios_cidadao.utils import (
    download_unzip_csv,
    extract_dates,
    parquet_partition,
)
from pipelines.utils.utils import extract_last_date, log


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
)
def check_for_updates(dataset_id: str, table_id: str, max_date: datetime) -> bool:
    """
    Verifica se há atualizações disponíveis em um conjunto de dados (dataset).

    Parâmetros:
    - dataset_id (str): O identificador do conjunto de dados no site.
    - table_id (str): O identificador da tabela dentro do conjunto de dados.
    - max_date (datetime): A data mais recente a ser comparada com a última data na BD.

    Retorna:
    - True se houver atualizações disponíveis, False caso contrário.

    Exemplo de uso:
    if check_for_updates("meu_dataset", "minha_tabela", datetime(2023, 11, 10)):
        print("Há atualizações disponíveis.")
    else:
        print("Não há novas atualizações disponíveis.")
    """

    # Obtém a última data no site BD
    bd_date = extract_last_date(
        dataset_id,
        table_id,
        "yy-mm",
        billing_project_id=main_constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
    )
    bd_date = datetime.strptime(bd_date, "%Y-%m")
    # Registra a data mais recente do site
    log(f"Última data no Portal da Transferência: {max_date}")
    log(f"Última data na BD: {bd_date}")

    # Compara as datas para verificar se há atualizações
    if max_date > bd_date:
        log("Há atualizações disponíveis")
        return True  # Há atualizações disponíveis
    else:
        log("Não há novas atualizações disponíveis")
        return False  # Não há novas atualizações disponíveis


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)  # noqa
def crawler_bolsa_familia(historical_data: bool, file):
    if historical_data:
        dates = extract_dates()

        endpoints = dates["urls"].to_list()

        log("BAIXANDO DADOS HISTÓRICOS")
        log(f"ENDPOINTS >> {endpoints}")

        download_unzip_csv(
            url=constants.MAIN_URL_NOVO_BOLSA_FAMILIA.value, files=endpoints, id="dados"
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
def crawler_garantia_safra(historical_data: bool, file):
    if historical_data:
        dates = extract_dates(table="garantia_safra")

        endpoints = dates["urls"].to_list()

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
def crawler_bpc(historical_data: bool, file):
    if historical_data:
        dates = extract_dates(table="bpc")

        endpoints = dates["urls"].to_list()

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
