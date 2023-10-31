# -*- coding: utf-8 -*-
"""
Tasks for br_cgu_bolsa_familia
"""

from datetime import datetime

from prefect import task

from pipelines.datasets.br_cgu_beneficios_cidadao.constants import constants
from pipelines.datasets.br_cgu_beneficios_cidadao.utils import (
    download_unzip_csv,
    extract_dates,
    parquet_partition,
)
from pipelines.utils.utils import get_credentials_from_secret, log


@task  # noqa
def crawler_bolsa_familia(historical_data: bool):
    if historical_data:
        dates = extract_dates()

        endpoints = dates["urls"].to_list()

        log("DOWNLOADING HISTORICAL DATA")
        log(f"ENDPOINTS >> {endpoints}")

        download_unzip_csv(url=constants.MAIN_URL.value, files=endpoints, id="dados")
    else:
        # TODO: inserir lógica de atualização:
        log("TODO")
        log("DOWNLOADING MOST RECENT DATA")
        download_unzip_csv(
            url=constants.MAIN_URL.value,
            files="202306_NovoBolsaFamilia.zip",
            id="dados",
        )

    parquet_partition(path="/tmp/data/br_cgu_bolsa_familia/dados/input/")
    return "/tmp/data/br_cgu_bolsa_familia/output/"


@task
def get_today_date():
    d = datetime.today()

    return d.strftime("%Y-%m")
