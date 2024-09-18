# -*- coding: utf-8 -*-
"""
Tasks for br_cgu_cartao_pagamento
"""
from prefect import task
from datetime import datetime
import pandas as pd
from pipelines.utils.utils import log, to_partitions
from pipelines.utils.crawler_cgu.utils import read_csv
from pipelines.utils.crawler_cgu.constants import constants
from pipelines.utils.crawler_cgu.utils import download_file
import basedosdados as bd

@task
def partition_data(table_id: str, year: str, month: str) -> str:
    """
    Partition data from a given table
    """

    value_constants = constants.TABELA.value[table_id]

    log("---------------------------- Read data ----------------------------")
    # Read the data
    df = read_csv(table_id = table_id,
                year = year,
                month = month,
                url = value_constants['URL'])

    # Partition the data
    log(" ---------------------------- Partiting data -----------------------")

    to_partitions(
        data = df,
        partition_columns=['ANO_EXTRATO', 'MES_EXTRATO'],
        savepath = value_constants['OUTPUT_DATA'],
        file_type='csv')

    log("---------------------------- Data partitioned ----------------------")

    return value_constants['OUTPUT_DATA']

@task
def get_max_date(table_id, year, month):
    """
    Get the maximum date from a given table for a specific year and month.

    Args:
        table_id (str): The ID of the table.
        year (int): The year.
        month (int): The month.

    Returns:
        datetime: The maximum date as a datetime object.
    """

    max_date = str(download_file(table_id, year, month))

    if len(max_date) == 10:
        pass

    elif len(max_date) == 6:
        max_date = max_date[0:4] + '-' + max_date[4:6] + '-01'

    date = datetime.strptime(max_date, '%Y-%m-%d')

    return date