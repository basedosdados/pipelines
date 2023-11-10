# -*- coding: utf-8 -*-
"""
Tasks for br_cgu_beneficios_cidadao
"""

from datetime import datetime

import pandas as pd
import requests
from prefect import task

from pipelines.datasets.br_cgu_beneficios_cidadao.constants import constants
from pipelines.datasets.br_cgu_beneficios_cidadao.utils import (
    download_unzip_csv,
    extract_dates,
    parquet_partition,
)
from pipelines.utils.utils import extract_last_date, get_credentials_from_secret, log


@task
def print_last_file(file):
    log(f"arquivo a ser baixado --> {file}")


@task
def crawl_last_date(dataset_id: str, table_id: str):
    dates = extract_dates(table=table_id)
    dates["data"] = pd.to_datetime(
        dates["ano"].astype(str) + "-" + dates["mes_numero"].astype(str) + "-01"
    )
    max_row = dates[dates["data"] == dates["data"].max()]

    max_date = max_row["data"].iloc[0]

    return [max_date, max_row["urls"].iloc[0]]


@task
def check_for_updates(dataset_id: str, table_id: str, max_date: datetime) -> bool:
    """ """

    # Obtém a última data no site BD
    bd_date = extract_last_date(dataset_id, table_id, "yy-mm", "basedosdados-dev")
    bd_date = datetime.strptime(bd_date, "%Y-%m")
    # Registra a data mais recente do site
    log(f"Última data no Portal da Transferência: {max_date}")
    log(f"Última data na BD: {bd_date}")

    # Compara as datas para verificar se há atualizações
    if max_date > bd_date:
        return True  # Há atualizações disponíveis
    else:
        return False  # Não há novas atualizações disponíveis


@task
def teste_selenium():
    dates = extract_dates(table="bpc")

    return log(dates["urls"].to_list())


@task  # noqa
def crawler_bolsa_familia(historical_data: bool):
    if historical_data:
        dates = extract_dates()

        endpoints = dates["urls"].to_list()

        log("DOWNLOADING HISTORICAL DATA")
        log(f"ENDPOINTS >> {endpoints}")

        download_unzip_csv(
            url=constants.MAIN_URL_NOVO_BOLSA_FAMILIA.value, files=endpoints, id="dados"
        )
    else:
        log("DOWNLOADING MOST RECENT DATA")
        download_unzip_csv(
            url=constants.MAIN_URL_NOVO_BOLSA_FAMILIA.value,
            files="202306_NovoBolsaFamilia.zip",
            id="novo_bolsa_familia",
        )

    parquet_partition(
        path="/tmp/data/br_cgu_beneficios_cidadao/novo_bolsa_familia/input/",
        table="novo_bolsa_familia",
    )
    return "/tmp/data/br_cgu_beneficios_cidadao/novo_bolsa_familia/output/"


@task  # noqa
def crawler_garantia_safra(historical_data: bool):
    if historical_data:
        dates = extract_dates(table="garantia_safra")

        endpoints = dates["urls"].to_list()

        log("DOWNLOADING HISTORICAL DATA")
        log(f"ENDPOINTS >> {endpoints}")

        download_unzip_csv(
            url=constants.MAIN_URL_GARANTIA_SAFRA.value,
            files=endpoints,
            id="garantia_safra",
        )
    else:
        # TODO: inserir lógica de atualização:
        log("TODO")
        log("DOWNLOADING MOST RECENT DATA")
        download_unzip_csv(
            url=constants.MAIN_URL_GARANTIA_SAFRA.value,
            files="202301_GarantiaSafra.zip",
            id="garantia_safra",
        )

    parquet_partition(
        path="/tmp/data/br_cgu_beneficios_cidadao/garantia_safra/input/",
        table="garantia_safra",
    )
    return "/tmp/data/br_cgu_beneficios_cidadao/garantia_safra/output/"


@task  # noqa
def crawler_bpc(historical_data: bool, file):
    if historical_data:
        dates = extract_dates(table="bpc")

        endpoints = dates["urls"].to_list()

        log("DOWNLOADING HISTORICAL DATA")
        log(f"ENDPOINTS >> {endpoints}")

        download_unzip_csv(
            url=constants.MAIN_URL_BPC.value,
            files=endpoints,
            id="bpc",
        )
    else:
        log("DOWNLOADING MOST RECENT DATA")
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


@task
def get_today_date():
    d = datetime.today()

    return d.strftime("%Y-%m")
