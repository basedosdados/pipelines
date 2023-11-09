# -*- coding: utf-8 -*-
"""
Tasks for br_cgu_beneficios_cidadao
"""

import io
import os
import zipfile
from datetime import datetime

import requests
from prefect import task

from pipelines.datasets.br_cgu_beneficios_cidadao.constants import constants
from pipelines.datasets.br_cgu_beneficios_cidadao.utils import (
    download_unzip_csv,
    extract_dates,
    parquet_partition,
)
from pipelines.utils.utils import get_credentials_from_secret, log


@task
def setup_web_driver() -> None:
    r = requests.get(constants.CHROME_DRIVER.value, stream=True)
    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
        z.extractall(constants.PATH.value)

    os.environ["PATH"] += os.pathsep + constants.PATH.value


@task
def teste_selenium():
    dates = extract_dates(table="bpc")

    return dates["urls"].to_list()


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
        # TODO: inserir lógica de atualização:
        log("TODO")
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
def crawler_bpc(historical_data: bool):
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
        # TODO: inserir lógica de atualização:
        log("TODO")
        log("DOWNLOADING MOST RECENT DATA")
        download_unzip_csv(
            url=constants.MAIN_URL_BPC.value,
            files="202301_BPC.zip",
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
