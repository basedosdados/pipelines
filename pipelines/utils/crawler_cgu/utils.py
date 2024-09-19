# -*- coding: utf-8 -*-
"""
General purpose functions for the br_cgu_cartao_pagamento project
"""
import datetime
import pandas as pd
import os
import basedosdados as bd
import requests
from pipelines.utils.crawler_cgu.constants import constants
from typing import List
import unidecode
from pipelines.utils.utils import log, download_and_unzip_file
from pipelines.utils.metadata.utils import get_api_most_recent_date, get_url


def download_file(table_id : str, year : str, month : str) -> None:
    """
    Download a file from a given URL and save it to a given path
    """

    value_constants = constants.TABELA.value[table_id]
    input = value_constants['INPUT_DATA']
    if not os.path.exists(input):
        os.makedirs(input)
    log(f' ---------------------------- Year = {year} --------------------------------------')
    log(f' ---------------------------- Month = {month} ------------------------------------')
    log(f' --------------------- URL = {value_constants["INPUT_DATA"]} ---------------------')
    if not value_constants['ONLY_ONE_FILE']:

        url = f"{value_constants['URL']}{year}{str(month).zfill(2)}/"

        status = requests.get(url).status_code == 200
        if status:
            log(f'------------------ URL = {url} ------------------')
            download_and_unzip_file(url, value_constants['INPUT_DATA'])
            return url.split("/")[-2]

        else:
            log('URL não encontrada. Fazendo uma query na BD')
            log(f'------------------ URL = {url} ------------------')
            last_date = last_date_in_metadata(
                                dataset_id="br_cgu_cartao_pagamento",
                                table_id=table_id
                                )
            return last_date

    if value_constants['ONLY_ONE_FILE']:
        url = value_constants['URL']
        download_and_unzip_file(url, value_constants['INPUT_DATA'])
        return None



def read_csv(table_id : str, url : str, column_replace : List = ['VALOR_TRANSACAO']) -> pd.DataFrame:
    """
    Read a csv file from a given path
    """
    value_constants = constants.TABELA.value[table_id]

    # Read the file
    os.listdir(value_constants['INPUT_DATA'])

    get_file = [x for x in os.listdir(value_constants['INPUT_DATA']) if x.endswith('.csv')][0]

    df = pd.read_csv(get_file, sep=';', encoding='latin1')

    df.columns = [unidecode.unidecode(x).upper().replace(" ", "_") for x in df.columns]

    for list_column_replace in column_replace:
        df[list_column_replace] = df[list_column_replace].str.replace(",", ".").astype(float)

    return df

def last_date_in_metadata(dataset_id : str, table_id : str) -> datetime.date:

    backend = bd.Backend(graphql_url=get_url("prod"))
    last_date_in_api = get_api_most_recent_date(
        dataset_id=dataset_id,
        table_id=table_id,
        date_format="%Y-%m",
        backend=backend,
    )

    return last_date_in_api