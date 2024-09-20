# -*- coding: utf-8 -*-
"""
General purpose functions for the br_cgu_cartao_pagamento project
"""
import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
import os
import basedosdados as bd
import requests
from pipelines.utils.crawler_cgu.constants import constants
from typing import List
import unidecode
from pipelines.utils.utils import log, download_and_unzip_file
from pipelines.utils.metadata.utils import get_api_most_recent_date, get_url


def download_file(table_id : str, year : int, month : int, relative_month = int) -> None:
    """
        Downloads and unzips a file from a specified URL based on the given table ID, year, and month.

        Parameters:
        table_id (str): The identifier for the table to download data for.
        year (int): The year for which data is to be downloaded.
        month (int): The month for which data is to be downloaded.
        relative_month (int): The relative month used for querying metadata.

        Returns:
        None: If the file is successfully downloaded and unzipped.
        str: The next date in the API if the URL is found.
        str: The last date in the API if the URL is not found.
        """
    value_constants = constants.TABELA.value[table_id]
    input = value_constants['INPUT_DATA']
    if not os.path.exists(input):
        os.makedirs(input)

    log(f' ---------------------------- Year = {year} --------------------------------------')
    log(f' ---------------------------- Month = {month} ------------------------------------')

    if not value_constants['ONLY_ONE_FILE']:

        url = f"{value_constants['URL']}{year}{str(month).zfill(2)}/"

        status = requests.get(url).status_code == 200
        if status:
            log(f'------------------ URL = {url} ------------------')
            download_and_unzip_file(url, value_constants['INPUT_DATA'])

            last_date_in_api, next_date_in_api = last_date_in_metadata(
                                dataset_id="br_cgu_cartao_pagamento",
                                table_id=table_id,
                                relative_month=relative_month
                                )

            return next_date_in_api

        else:
            log('URL não encontrada. Fazendo uma query na BD')
            log(f'------------------ URL = {url} ------------------')

            last_date_in_api, next_date_in_api = last_date_in_metadata(
                                dataset_id="br_cgu_cartao_pagamento",
                                table_id=table_id,
                                relative_month=relative_month
                                )

            return last_date_in_api

    if value_constants['ONLY_ONE_FILE']:
        url = value_constants['URL']
        download_and_unzip_file(url, value_constants['INPUT_DATA'])
        return None



def read_csv(table_id : str, url : str, column_replace : List = ['VALOR_TRANSACAO']) -> pd.DataFrame:
    """
    Reads a CSV file from a specified path and processes its columns.

        Args:
            table_id (str): The identifier for the table to be read.
            url (str): The URL from which the CSV file is to be read.
            column_replace (List, optional): A list of column names whose values need to be replaced. Default is ['VALOR_TRANSACAO'].

        Returns:
            pd.DataFrame: A DataFrame containing the processed data from the CSV file.

        Notes:
            - The function reads the CSV file from a directory specified in a constants file.
            - It assumes the CSV file is encoded in 'latin1' and uses ';' as the separator.
            - Column names are converted to uppercase, spaces are replaced with underscores, and accents are removed.
            - For columns specified in `column_replace`, commas in their values are replaced  with dots and the values are converted to float.
    """
    value_constants = constants.TABELA.value[table_id]

    os.listdir(value_constants['INPUT_DATA'])

    csv_file = [f for f in os.listdir(value_constants['INPUT_DATA']) if f.endswith('.csv')][0]
    log(f"CSV files: {csv_file}")

    df = pd.read_csv(f"{value_constants['INPUT_DATA']}/{csv_file}", sep=';', encoding='latin1')

    df.columns = [unidecode.unidecode(x).upper().replace(" ", "_") for x in df.columns]

    for list_column_replace in column_replace:
        df[list_column_replace] = df[list_column_replace].str.replace(",", ".").astype(float)

    return df

def last_date_in_metadata(dataset_id : str,
                        table_id : str,
                        relative_month) -> datetime.date:
    """
    Retrieves the most recent date from the metadata of a specified dataset and table,
    and calculates the next date based on a relative month offset.

    Args:
        dataset_id (str): The ID of the dataset to query.
        table_id (str): The ID of the table within the dataset to query.
        relative_month (int): The number of months to add to the most recent date to calculate the next date.

    Returns:
        tuple: A tuple containing:
            - last_date_in_api (datetime.date): The most recent date found in the API.
            - next_date_in_api (datetime.date): The date obtained by adding the relative month to the most recent date.
    """

    backend = bd.Backend(graphql_url=get_url("prod"))
    last_date_in_api = get_api_most_recent_date(
        dataset_id=dataset_id,
        table_id=table_id,
        date_format="%Y-%m",
        backend=backend,
    )

    next_date_in_api =  last_date_in_api + relativedelta(months=relative_month)

    return last_date_in_api, next_date_in_api