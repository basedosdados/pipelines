# -*- coding: utf-8 -*-
"""
Tasks for br_cgu_cartao_pagamento
"""
from datetime import datetime
from prefect import task
from dateutil.relativedelta import relativedelta
import pandas as pd
from pipelines.utils.utils import log, to_partitions
from pipelines.utils.crawler_cgu.utils import read_csv, last_date_in_metadata
from pipelines.utils.crawler_cgu.constants import constants
from pipelines.utils.crawler_cgu.utils import download_file
from typing import Tuple

@task
def partition_data(table_id: str) -> str:
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

    value_constants = constants.TABELA.value[table_id]

    log("---------------------------- Read data ----------------------------")
    # Read the data
    df = read_csv(table_id = table_id,
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


    max_date = str(download_file(table_id = table_id,
                                year = next_date_in_api.year,
                                month = next_date_in_api.month,
                                relative_month=relative_month))

    date = datetime.strptime(max_date, '%Y-%m-%d')

    return date
