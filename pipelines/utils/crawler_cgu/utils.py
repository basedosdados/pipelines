# -*- coding: utf-8 -*-
"""
General purpose functions for the br_cgu_cartao_pagamento project
"""

import pandas as pd
import os
import basedosdados as bd
import requests
from pipelines.utils.crawler_cgu.constants import constants
from typing import List
import unidecode
from pipelines.utils.utils import log, download_and_unzip_file

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
    if not value_constants['UNICO']:

        url = f"{value_constants['URL']}{year}{str(month).zfill(2)}/"

        status = requests.get(url).status_code == 200
        if status:
            log(f'------------------ URL = {url} ------------------')
            download_and_unzip_file(url, value_constants['INPUT_DATA'])
            return url.split("/")[-2]

        else:
            log('URL não encontrada. Fazendo uma query na BD')
            log(f'------------------ URL = {url} ------------------')
            query_bd = bd.read_sql(f"select max(date(ano_extrato, mes_extrato, 1)) as valor_maximo from `basedosdados.br_cgu_cartao_pagamento.{table_id}`",
                            billing_project_id="basedosdados",
                            from_file=True)

            return query_bd["valor_maximo"][0]

    if value_constants['UNICO']:
        url = value_constants['URL']
        download_and_unzip_file(url, value_constants['INPUT_DATA'])
        return None



def read_csv(table_id : str, url : str, year : str, month : str, column_replace : List = ['VALOR_TRANSACAO']) -> pd.DataFrame:
    """
    Read a csv file from a given path
    """
    value_constants = constants.TABELA.value[table_id]

    # Read the file
    file_with_year_month = f"{value_constants['INPUT_DATA']}/{year}{str(month).zfill(2)}{value_constants['READ']}.csv"

    df = pd.read_csv(file_with_year_month, sep=';', encoding='latin1')

    df.columns = [unidecode.unidecode(x).upper().replace(" ", "_") for x in df.columns]

    for list_column_replace in column_replace:
        df[list_column_replace] = df[list_column_replace].str.replace(",", ".").astype(float)

    return df