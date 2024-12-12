# -*- coding: utf-8 -*-
"""
General purpose functions for the br_cgu_cartao_pagamento project
"""
import datetime
from arrow import get
from dateutil.relativedelta import relativedelta
import gc
import shutil
from functools import lru_cache
from rapidfuzz import process
import pandas as pd
import os
import unidecode
import basedosdados as bd
import requests
from dateutil.relativedelta import relativedelta
from pipelines.utils.crawler_cgu.constants import constants
from typing import List
from tqdm import tqdm
from pipelines.utils.utils import log, download_and_unzip_file
from pipelines.utils.metadata.utils import get_api_most_recent_date, get_url
from pipelines.utils.crawler_cgu.constants import constants
from pipelines.utils.apply_architecture_to_dataframe.utils import (
    read_architecture_table,
    rename_columns,
)
from pipelines.utils.utils import log
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from webdriver_manager.chrome import ChromeDriverManager


def build_urls(dataset_id: str, url: str, year: int, month: int, table_id: str) -> str:
    """
    Constructs URLs based on the provided parameters.

    Args:
        modelo (str): The model type which determines the URL structure.
        url (str): The base URL to which the year, month, and table name will be appended.
        year (int): The year to be included in the URL.
        month (int): The month to be included in the URL.
        table_id (str): The table identifier used to fetch specific table names for constructing URLs.

    Returns:
        str: A single URL string if the model is 'TABELA'.
        list: A list of URL strings if the model is 'TABELA_SERVIDORES'.
    """

    log(f"{dataset_id=}")

    if dataset_id in ["br_cgu_cartao_pagamento", "br_cgu_licitacao_contrato"]:
        log(f"{url}{year}{str(month).zfill(2)}/")

        return f"{url}{year}{str(month).zfill(2)}/"

    elif dataset_id == "br_cgu_servidores_executivo_federal":
        list_url = []
        for table_name in constants.TABELA_SERVIDORES.value[table_id]["READ"].keys():
            url_completa = f"{url}{year}{str(month).zfill(2)}_{table_name}/"
            list_url.append(url_completa)
        return list_url

def build_input(table_id):
    """
    Builds a list of input directories based on the given table ID.

    This function retrieves the input keys from the constants.TABELA_SERVIDORES
    dictionary for the specified table_id. It then checks if each input directory
    exists, creates it if it does not, and appends the directory path to a list.
    Finally, it logs the list of input directories and returns it.

    Args:
        table_id (str): The ID of the table for which to build the input directories.

    Returns:
        list: A list of input directory paths.

    Raises:
        KeyError: If the table_id is not found in constants.TABELA_SERVIDORES.
    """
    list_input = []
    for input in constants.TABELA_SERVIDORES.value[table_id]["READ"].keys():

        value_input = f"{input}"
        if not os.path.exists(value_input):
            os.makedirs(value_input)
            print(value_input)
        list_input.append(value_input)
    log(f"Lista de inputs: {list_input}")
    return list_input


def download_file(dataset_id: str, table_id: str, year: int, month: int, relative_month=int) -> None:
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

    if dataset_id in ["br_cgu_cartao_pagamento", "br_cgu_licitacao_contrato"]:
        if dataset_id == "br_cgu_cartao_pagamento":
            value_constants = constants.TABELA.value[table_id] # ! CGU - Cartão de Pagamento
        elif dataset_id == "br_cgu_licitacao_contrato":
            value_constants = constants.TABELA_LICITACAO_CONTRATO.value[table_id]

        input = value_constants["INPUT"]
        if not os.path.exists(input):
            os.makedirs(input)

        url: str = build_urls(
            url=value_constants["URL"],
            year=year,
            month=month,
            table_id=table_id,
            dataset_id=dataset_id,
        )
        log(url)

        status = requests.get(url).status_code == 200
        if status:
            log(f'------------------ URL = {url} ------------------')
            download_and_unzip_file(url, value_constants['INPUT'])

            last_date_in_api, next_date_in_api = last_date_in_metadata(
                                dataset_id=dataset_id,
                                table_id=table_id,
                                relative_month=relative_month
                                )

            return next_date_in_api

        else:
            log('URL não encontrada. Fazendo uma query na BD')
            log(f'------------------ URL = {url} ------------------')

            last_date_in_api, next_date_in_api = last_date_in_metadata(
                                dataset_id=dataset_id,
                                table_id=table_id,
                                relative_month=relative_month
                                )

            return last_date_in_api

    elif dataset_id == "br_cgu_servidores_executivo_federal":

        constants_cgu_servidores = constants.TABELA_SERVIDORES.value[table_id]  # ! CGU - Servidores Públicos do Executivo Federal

        url = build_urls(
            dataset_id,
            constants.URL_SERVIDORES.value,
            year,
            month,
            table_id,
        )
        input_dirs = build_input(table_id)
        log(url)
        log(input_dirs)
        for urls, input_dir in zip(url, input_dirs):
            if requests.get(urls).status_code == 200:
                destino = f"{constants_cgu_servidores['INPUT']}/{input_dir}"
                download_and_unzip_file(urls, destino)

                last_date_in_api, next_date_in_api = last_date_in_metadata(
                    dataset_id="br_cgu_servidores_executivo_federal",
                    table_id=table_id,
                    relative_month=relative_month,
                )

        return next_date_in_api


# Função para carregar o dataframe
@lru_cache(maxsize=1)  # Cache para evitar recarregar a tabela
def load_municipio() -> None:
    municipio: pd.DataFrame = bd.read_table(
        "br_bd_diretorios_brasil",
        "municipio",
        billing_project_id="basedosdados",
        from_file=True,
    )
    municipio["cidade_uf"] = (
        municipio["nome"].apply(lambda x: x.upper()) + "-" + municipio["sigla_uf"]
    )
    return municipio


def get_similar_cities_process(city):
    municipio = load_municipio()
    results = process.extractOne(city, municipio["cidade_uf"], score_cutoff=70)
    return results[0] if results else None


def read_csv(
    dataset_id : str, table_id: str, column_replace: List = ["VALOR_TRANSACAO"]
) -> pd.DataFrame:
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

    if dataset_id == "br_cgu_cartao_pagamento":
        value_constants = constants.TABELA.value[table_id]

        os.listdir(value_constants["INPUT"])

        csv_file = [
            f for f in os.listdir(value_constants["INPUT"]) if f.endswith(".csv")
        ][0]
        log(f"CSV files: {csv_file}")

        df = pd.read_csv(
            f"{value_constants['INPUT']}/{csv_file}", sep=";", encoding="latin1"
        )

        df.columns = [unidecode.unidecode(x).upper().replace(" ", "_") for x in df.columns]

        for list_column_replace in column_replace:
            df[list_column_replace] = (
                df[list_column_replace].str.replace(",", ".").astype(float)
            )

        return df

    if dataset_id == "br_cgu_licitacao_contrato":
        constants_cgu_licitacao_contrato = constants.TABELA_LICITACAO_CONTRATO.value[table_id]
        print(os.listdir(constants_cgu_licitacao_contrato["INPUT"]))
        csv_file = [
            f
            for f in os.listdir(constants_cgu_licitacao_contrato["INPUT"])
            if f.endswith(constants_cgu_licitacao_contrato["READ"])
        ][0]
        log(f"CSV files: {csv_file}")
        log(f"{constants_cgu_licitacao_contrato['INPUT']}/{csv_file}")
        df = pd.read_csv(f"{constants_cgu_licitacao_contrato['INPUT']}/{csv_file}", sep=";", encoding="latin1")
        df['ano'] = csv_file[:4]
        df['mes'] = csv_file[4:6]

        df.columns = [unidecode.unidecode(col) for col in df.columns]
        df.columns = [col.replace(" ", "_").lower() for col in df.columns]

        if table_id == "licitacao":
                df["cidade_uf"] = df["municipio"] + "-" + df["uf"]

                df["cidade_uf_dir"] = df["cidade_uf"].apply(
                    lambda x: get_similar_cities_process(x)
                )
                df.drop(["cidade_uf", "municipio"], axis=1, inplace=True)

                df.rename(columns={"cidade_uf_dir": "municipio"}, inplace=True)

                df["municipio"] = df["municipio"].apply(lambda x: x if x == None else x.split("-")[0])

        return df

def last_date_in_metadata(
    dataset_id: str, table_id: str, relative_month
) -> datetime.date:
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

    next_date_in_api = last_date_in_api + relativedelta(months=relative_month)

    return last_date_in_api, next_date_in_api

def create_column_ano(df: pd.DataFrame, csv_path: str) -> pd.DataFrame:
    """
    Adds a new column 'ano' to the DataFrame based on the first four characters of the csv_path.

    Parameters:
    df (pd.DataFrame): The input DataFrame to which the new column will be added.
    csv_path (str): The file path string from which the year will be extracted.

    Returns:
    pd.DataFrame: The DataFrame with the added 'ano' column.
    """
    df['ano'] = int(csv_path[:4])
    return df

def create_column_month(df : pd.DataFrame, csv_path : str) -> str:
    """
    Adds a 'mes' column to the DataFrame based on the month extracted from the csv_path.

    Args:
        df (pd.DataFrame): The input DataFrame to which the 'mes' column will be added.
        csv_path (str): The file path string from which the month will be extracted.
                        It is assumed that the month is represented by characters at positions 4 and 5.

    Returns:
        pd.DataFrame: The DataFrame with the added 'mes' column.
    """
    df["mes"] = int(csv_path[4:6])

    return df


def exclude_used_tables(path: str) -> None:
    """
    Deletes the directory at the specified path if it exists and performs garbage collection.

    Args:
        path (str): The path to the directory to be deleted.

    Returns:
        None
    """
    if os.path.exists(path):
        shutil.rmtree(path)
    gc.collect()

    return None


def read_and_clean_csv(table_id: str) -> pd.DataFrame:
    """
    Reads and cleans CSV files for a given table ID.

    This function performs the following steps:
    1. Builds the input path for the given table ID.
    2. Iterates through the CSV files in the input path.
    3. Reads each CSV file into a DataFrame.
    4. Renames columns based on a predefined architecture.
    5. Adds 'ano' and 'month' columns based on the CSV file name.
    6. Adds an 'origem' column if it exists in the architecture.
    7. Appends the cleaned DataFrame to a list.
    8. Concatenates all DataFrames in the list into a single DataFrame.
    9. Returns the concatenated DataFrame.

    Args:
        table_id (str): The ID of the table to process.

    Returns:
        pd.DataFrame: The concatenated and cleaned DataFrame.
    """
    append_dataframe = []
    constants_cgu_servidores = constants.TABELA_SERVIDORES.value[table_id]
    for csv_path in build_input(table_id):
        path = f"{constants_cgu_servidores['INPUT']}/{csv_path}"
        for get_csv in os.listdir(path):
            if get_csv.endswith(f"{constants_cgu_servidores['NAME_TABLE']}") == True:
                log(f"Reading {table_id=}, {csv_path=}, {get_csv=}")
                df = pd.read_csv(
                    os.path.join(path, get_csv),
                    sep=";",
                    encoding="latin-1",
                ).rename(
                    columns=lambda col: col.replace("\x96 ", ""))
                url_architecture = constants_cgu_servidores["ARCHITECTURE"]
                df_architecture = read_architecture_table(url_architecture)
                df = rename_columns(df, df_architecture)

                create_column_ano(df, get_csv)
                create_column_month(df, get_csv)
                if "origem" in df_architecture["name"].to_list():
                    df["origem"] = get_source(table_id, csv_path)

                    append_dataframe.append(df)

                exclude_used_tables(path)

    if len(append_dataframe) > 1:
        df = pd.concat(append_dataframe)
    else:
        df

    return df

def get_source(table_name: str, source: str) -> str:
    ORIGINS = {
        "cadastro_aposentados": {
            "Aposentados_BACEN": "BACEN",
            "Aposentados_SIAPE": "SIAPE",
        },
        "cadastro_pensionistas": {
            "Pensionistas_SIAPE": "SIAPE",
            "Pensionistas_DEFESA": "Defesa",
            "Pensionistas_BACEN": "BACEN",
        },
        "cadastro_servidores": {
            "Servidores_BACEN": "BACEN",
            "Servidores_SIAPE": "SIAPE",
            "Militares": "Militares",
        },
        "cadastro_reserva_reforma_militares": {
            "Reserva_Reforma_Militares": "Reserva Reforma Militares"
        },
        "remuneracao": {
            "Militares": "Militares",
            "Pensionistas_BACEN": "Pensionistas BACEN",
            "Pensionistas_DEFESA": "Pensionistas DEFESA",
            "Reserva_Reforma_Militares": "Reserva Reforma Militares",
            "Servidores_BACEN": "Servidores BACEN",
            "Servidores_SIAPE": "Servidores SIAPE",
        },
        "afastamentos": {
                        "Servidores_BACEN": "BACEN",
                        "Servidores_SIAPE": "SIAPE"},
        "observacoes": {
            "Aposentados_BACEN": "Aposentados BACEN",
            "Aposentados_SIAPE": "Aposentados SIAPE",
            "Militares": "Militares",
            "Pensionistas_BACEN": "Pensionistas BACEN",
            "Pensionistas_DEFESA": "Pensionistas DEFESA",
            "Pensionistas_SIAPE": "Pensionistas SIAPE",
            "Reserva_Reforma_Militares": "Reserva Reforma Militares",
            "Servidores_BACEN": "Servidores BACEN",
            "Servidores_SIAPE": "Servidores SIAPE",
        },
    }

    return ORIGINS[table_name][source]
