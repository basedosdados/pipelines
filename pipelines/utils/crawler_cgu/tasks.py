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

@task
def partition_data(table_id: str, year: str, month: str) -> str:
    """
    Partition data from a given table
    """

    flow_unico = constants.TABELA.value[table_id]

    log("Partitioning data")
    # Read the data
    df = read_csv(table_id = table_id,
                year = year,
                month = month,
                url = flow_unico['URL'])
    # Partition the data
    to_partitions(
        data = df,
        partition_columns=['ANO_EXTRATO', 'MES_EXTRATO'],
        savepath = flow_unico['OUTPUT'],
        file_type='csv')
    log("Data partitioned")

    return flow_unico['OUTPUT']

@task
def get_max_date(table_id, year, month):
    max_date = download_file(table_id, year, month)

    fix_date = max_date[:4] + '-' + max_date[4:6] + '-' + '01'

    date = datetime.strptime(fix_date, '%Y-%m-%d')

    return date