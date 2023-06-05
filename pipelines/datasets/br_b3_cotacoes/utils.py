# -*- coding: utf-8 -*-
"""
General purpose functions for the br_bcb_estban project
"""
import requests
from lxml import html
import basedosdados as bd
from io import BytesIO
from zipfile import ZipFile
from urllib.request import urlopen
import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
from os.path import join
from pathlib import Path
from pipelines.utils.utils import (
    log,
)
from pipelines.constants import constants

# ------- macro etapa 1 download de dados


def download_and_unzip(url, path):
    """download and unzip a zip file

    Args:
        url (str): a URL
        path (str): a path to extract the zip file

    Returns:
        str: path to the extracted files
    """
    try:
        os.makedirs(path, exist_ok=True)

        http_response = urlopen(url)
        zipfile = ZipFile(BytesIO(http_response.read()))
        zipfile.extractall(path=path)

        return path
    except:
        print("Erro ao baixar o arquivo, tentando novamente...")
        input_url = input("Digite a data de ontem no formato aaaa-mm-dd: ")
        url_completa = f"https://arquivos.b3.com.br/apinegocios/tickercsv/{input_url}"

        os.makedirs(path, exist_ok=True)

        http_response = urlopen(url_completa)
        zipfile = ZipFile(BytesIO(http_response.read()))
        zipfile.extractall(path=path)

        return path


# ------- macro etapa 2 tratamento de dados
# --- read files
def read_files(path: str) -> pd.DataFrame:
    """This function reads a file from a given path

    Args:
        path (str): a path to a file

    Returns:
        pd.DataFrame: a dataframe with the file data
    """
    try:
        df = pd.read_csv(path, sep=";")
        return df

    except FileNotFoundError:
        print(f"File not found: {path}")
        user_input = input("Digite a data de ontem no formato dd-mm-aaaa: ")
        df = pd.read_csv(
            f"/home/tricktx/basedosdados/br_b3_cotacoes/{user_input}_NEGOCIOSAVISTA.txt",
            sep=";",
        )
        return df


def partition_data(df: pd.DataFrame, column_name: list[str], output_directory: str):
    """
    Particiona os dados em subconjuntos de acordo com os valores únicos de uma coluna.
    Salva cada subconjunto em um arquivo CSV separado.
    df: DataFrame a ser particionado
    column_name: nome da coluna a ser usada para particionar os dados
    output_directory: diretório onde os arquivos CSV serão salvos
    """
    unique_values = df[column_name].unique()
    log(f"Valores únicos: {unique_values}")
    for value in unique_values:
        value_str = str(value)[:10]
        date_value = datetime.strptime(value_str, "%Y-%m-%d").date()
        log(date_value)
        formatted_value = date_value.strftime("%Y-%m-%d")
        log(formatted_value)

        partition_path = os.path.join(
            output_directory, f"{column_name}={formatted_value}"
        )
        log(f"Salvando dados em {partition_path}")
        if not os.path.exists(partition_path):
            os.makedirs(partition_path)

        df_partition = df[df[column_name] == value].copy()

        df_partition.drop([column_name], axis=1, inplace=True)
        log(f"df_partition: {df_partition}")
        csv_path = os.path.join(partition_path, "data.csv")
        df_partition.to_csv(csv_path, index=False, encoding="utf-8", na_rep="")
        log(f"Arquivo {csv_path} salvo com sucesso!")
