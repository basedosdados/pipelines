# -*- coding: utf-8 -*-
"""
Tasks for br_cgu_cartao_pagamento
"""
from datetime import datetime
from prefect import task
import os
import basedosdados as bd
import requests
import pandas as pd
from dateutil.relativedelta import relativedelta
from pipelines.utils.utils import log, to_partitions, download_and_unzip_file
from pipelines.utils.metadata.utils import get_api_most_recent_date, get_url
from pipelines.utils.crawler_cgu.utils import (
    read_csv,
    last_date_in_metadata,
    read_and_clean_csv,
    build_urls,
)
from pipelines.utils.crawler_cgu.constants import constants
from pipelines.utils.crawler_cgu.utils import download_file
from typing import Tuple


@task
def partition_data(table_id: str, dataset_id : str) -> str:
    """
    Partition data from a given table.

    This function reads data from a specified table, partitions it based on
    the columns 'ANO_EXTRATO' and 'MES_EXTRATO', and saves the partitioned
    data to a specified output path.

    Args:
        table_id (str): The identifier of the table to be partitioned.

    Returns:
        str: The path where the partitioned data is saved.
    """

    if dataset_id in ["br_cgu_cartao_pagamento", "br_cgu_licitacao_contrato"]:
        log("---------------------------- Read data ----------------------------")
        df = read_csv(dataset_id = dataset_id, table_id = table_id)
        log(df.head())
        if dataset_id == "br_cgu_cartao_pagamento":
            log(" ---------------------------- Partiting data -----------------------")
            to_partitions(
                data = df,
                partition_columns=['ANO_EXTRATO', 'MES_EXTRATO'],
                savepath = constants.TABELA.value[table_id]['OUTPUT'],
                file_type='csv')

            log("---------------------------- Data partitioned ----------------------")
            return constants.TABELA.value[table_id]['OUTPUT']

        if dataset_id == "br_cgu_licitacao_contrato":
            log(" ---------------------------- Partiting data -----------------------")
            to_partitions(
                data=df,
                partition_columns=["ano", "mes"],
                savepath=constants.TABELA_LICITACAO_CONTRATO.value[table_id]["OUTPUT"],
                file_type="csv",
            )
            log("---------------------------- Data partitioned ----------------------")
            return constants.TABELA_LICITACAO_CONTRATO.value[table_id]["OUTPUT"]

    elif dataset_id == "br_cgu_servidores_executivo_federal":

        log("---------------------------- Read data ----------------------------")
        df = read_and_clean_csv(table_id = table_id)
        log(" ---------------------------- Partiting data -----------------------")
        to_partitions(
            data=df,
            partition_columns=["ano", "mes"],
            savepath=constants.TABELA_SERVIDORES.value[table_id]['OUTPUT'],
        )
        log("---------------------------- Data partitioned ----------------------")
        return constants.TABELA_SERVIDORES.value[table_id]['OUTPUT']


@task
def get_current_date_and_download_file(table_id : str,
                                        dataset_id : str,
                                        relative_month : int = 1) -> datetime:
    """
    Get the maximum date from a given table for a specific year and month.

    Args:
        table_id (str): The ID of the table.
        year (int): The year.
        month (int): The month.

    Returns:
        datetime: The maximum date as a datetime object.
    """
    last_date_in_api, next_date_in_api = last_date_in_metadata(
                                    dataset_id = dataset_id,
                                    table_id = table_id,
                                    relative_month = relative_month
                                    )
    log(f"Last date in API: {last_date_in_api}")
    log(f"Next date in API: {next_date_in_api}")

    max_date = str(download_file(table_id = table_id,
                                dataset_id = dataset_id,
                                year = next_date_in_api.year,
                                month = next_date_in_api.month,
                                relative_month=relative_month))

    log(f"Max date: {max_date}")

    date = datetime.strptime(max_date, '%Y-%m-%d')

    return date


@task
def verify_all_url_exists_to_download(dataset_id, table_id, relative_month) -> bool:
    """
    Verifies if all URLs are valid and can be downloaded.

    Args:
        table_id (str): The identifier for the table to download data for.
        year (int): The year for which data is to be downloaded.
        month (int): The month for which data is to be downloaded.

    Returns:
        bool: True if all URLs are valid and can be downloaded, False otherwise.
    """
    last_date_in_api, next_date_in_api = last_date_in_metadata(
        dataset_id=dataset_id, table_id=table_id, relative_month=relative_month
    )

    urls = build_urls(
        dataset_id,
        constants.URL_SERVIDORES.value,
        next_date_in_api.year,
        next_date_in_api.month,
        table_id,
    )

    for url in urls:
        log(f"Verificando se a URL {url=} existe")
        if requests.get(url).status_code != 200:
            log(f"A URL {url=} não existe!")
            return False

        log(f"A URL {url=} existe!")
    return True
