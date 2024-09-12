# -*- coding: utf-8 -*-
"""
General purpose functions for the br_cgu_cartao_pagamento project
"""

import pandas as pd
import os
from pipelines.utils.crawler_cgu.constants import constants
from urllib.request import urlopen
from io import BytesIO
import zipfile
from typing import List
import unidecode

def download_and_unzip_file(url : str, path : str) -> None:
    print("------------------ Baixando e descompactando arquivo ------------------")
    try:
        r = urlopen(url)
        zip = zipfile.ZipFile(BytesIO(r.read()))
        zip.extractall(path=path)
    except Exception as e:
        print(e)
        print("Erro ao baixar e descompactar arquivo")

def download_file(table_id : str, year : str, month : str) -> None:
    """
    Download a file from a given URL and save it to a given path
    """
    flow_unico = constants.TABELA.value[table_id]
    if not flow_unico['UNICO']:

        url = f"{flow_unico['URL']}{year}{str(month).zfill(2)}/"
        print(f'------------------ {url} ------------------')

        download_and_unzip_file(url, flow_unico['INPUT'])

        return url.split("/")[-2]

    url = flow_unico['URL']
    download_and_unzip_file(url, flow_unico['INPUT'])
    return None


def read_csv(table_id : str, url : str, year : str, month : str, column_replace : List = ['VALOR_TRANSACAO']) -> pd.DataFrame:
    """
    Read a csv file from a given path
    """

    flow_unico = constants.TABELA.value[table_id]
    # Check if the file exists
    input = flow_unico['INPUT']
    print(f"Criando diretório: {input}")
    if not os.path.exists(input):
        os.makedirs(input)
    print(f' --------------------- {year} ---------------------')
    print(f' --------------------- {month} ---------------------')
    print(f' --------------------- {flow_unico["READ"]} ---------------------')
    # Read the file
    file_with_year_month = f"{input}/{year}{month}{flow_unico['READ']}.csv"
    print(file_with_year_month)

    df = pd.read_csv(filepath_or_buffer=file_with_year_month, sep=';', encoding='latin1')

    df.columns = [unidecode.unidecode(x).upper().replace(" ", "_") for x in df.columns]

    for list_column_replace in column_replace:
        df[list_column_replace] = df[list_column_replace].str.replace(",", ".").astype(float)

    df.columns

    return df