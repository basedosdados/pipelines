# -*- coding: utf-8 -*-
"""
General purpose functions for the br_ons_estimativa_custos project
"""

import os
import re
import time as tm
import unicodedata
from datetime import datetime
from io import StringIO
from typing import List

import pandas as pd
import requests
import wget
from bs4 import BeautifulSoup


def extrai_data_recente(df: pd.DataFrame, table_name: str):
    """
    Extracts the last update date of a given dataset table.

    Args:
        dataset_id (str): The ID of the dataset.
        table_id (str): The ID of the table.
        billing_project_id (str): The billing project ID.

    Returns:
        str: The last update date in the format 'yyyy-mm' or 'yyyy-mm-dd'.

    Raises:
        Exception: If an error occurs while extracting the last update date.
    """

    # if else para os casos
    # ver em qual parte do corpo inserir

    date_dict = {
        "reservatorio": "yyyy-mm-dd",
        "geracao_usina": "yyyy-mm-dd hh:mm:ss",
        "geracao_termica_motivo_despacho": "yyyy-mm-dd hh:mm:ss",
        "energia_natural_afluente": "yyyy-mm-dd",
        "energia_armazenada_reservatorio": "yyyy-mm-dd",
        "restricao_operacao_usinas_eolicas": "yyyy-mm-dd hh:mm:ss",
        "custo_marginal_operacao_semi_horario": "yyyy-mm-dd hh:mm:ss",
        "custo_marginal_operacao_semanal": "yyyy-mm-dd",
        "balanco_energia_subsistemas": "yyyy-mm-dd hh:mm:ss",
        "balanco_energia_subsistemas_dessem": "yyyy-mm-dd hh:mm:ss",
        "custo_variavel_unitario_usinas_termicas": "yyyy-mm-dd",
    }

    if (
        date_dict[table_name] == "yyyy-mm-dd"
        and table_name != "custo_variavel_unitario_usinas_termicas"
    ):
        df["data"] = pd.to_datetime(df["data"], format="%Y-%m-%d").dt.date
        data = df["data"].max()

    if date_dict[table_name] == "yyyy-mm-dd hh:mm:ss":
        df["data_hora"] = df["data"] + " " + df["hora"]
        df["data_hora"] = pd.to_datetime(df["data_hora"], format="%Y-%m-%d %H:%M:%S")
        data = df["data_hora"].max()

    if (
        date_dict[table_name] == "yyyy-mm-dd"
        and table_name == "custo_variavel_unitario_usinas_termicas"
    ):
        df["data_inicio"] = pd.to_datetime(df["data_inicio"], format="%Y-%m-%d").dt.date
        data = df["data_inicio"].max()

    return data


def parse_year_or_year_month(url: str) -> str:
    # Extrai o ano e mês do link
    result = url.split("/")[-1].split(".")[-2]
    element1 = result.split("_")[-2]
    element2 = result.split("_")[-1]
    element_list = [element1, element2]
    print(element_list)

    elements = []
    # uma elemento vazio ocupa espaço?

    for element in element_list:
        if len(element) == 4 and re.match(r"^\d+$", element):
            print(f"O elemento -- {element} -- é provavelmente um -- ano --")
            elements.append(element)
        elif len(element) == 2 and re.match(r"^\d+$", element):
            print(f"O elemento -- {element} -- é provavelmente um -- mês --")
            elements.append(element)
        else:
            print(f"The element -- {element} -- is not a month nor a -- year --")

    # se a lista elements tiver comprimento 1, então é um ano
    if len(elements) == 1:
        date = datetime.strptime(elements[0], "%Y")

    # se a lista elements tiver comprimento 2, então é um ano e mes
    if len(elements) == 2:
        date = datetime.strptime(elements[0] + "-" + elements[1], "%Y-%m")

    return date


def crawler_ons(
    url: str,
) -> List[str]:
    """this function extract all download links from bcb agencias website
    Args:
        url (str): bcb url https://www.bcb.gov.br/fis/info/agencias.asp?frame=1

    Returns:
        list: a list of file links
    """
    # Send a GET request to the URL
    response = requests.get(url)

    # Parse the HTML content of the response using lxml
    html = response.text

    # Parse the HTML code
    soup = BeautifulSoup(html, "html.parser")

    # Find all 'a' elements with href containing ".csv"
    csv_links = soup.find_all("a", href=lambda href: href and href.endswith(".csv"))

    # Extract the href attribute from the csv_links
    table_urls = [link["href"] for link in csv_links]
    # table_names = [name["a"] for name in table_urls]

    # Print the csv_urls
    print(table_urls)
    # print(table_names)
    return table_urls


def download_data_final(
    path: str,
    url: str,
    table_name: str,
):
    """A simple crawler to download data from ONS  website.

    Args:
        path (str): the path to store the data
        url (str): the table URL from ONS website.
        table_name (str): the table name is the original name of the zip file with raw data from comex stat website
    """
    # selects a url given a table name

    # log(f"Downloading data from {url}")

    # downloads the file and saves it
    wget.download(url, out=path + table_name + "/input")
    # just for precaution,
    # sleep for 8 secs in between iterations
    tm.sleep(1)


def download_data(
    path: str,
    url_list: List[str],
    table_name: str,
):
    """A simple crawler to download data from comex stat website.

    Args:
        path (str): the path to store the data
        table_type (str): the table type is either ncm or mun. ncm stands for 'nomenclatura comum do mercosul' and
        mun for 'município'.
        table_name (str): the table name is the original name of the zip file with raw data from comex stat website
    """
    # selects a url given a table name
    for url in url_list:
        # log(f"Downloading data from {url}")

        # downloads the file and saves it
        wget.download(url, out=path + table_name + "/input")
        # just for precaution,
        # sleep for 8 secs in between iterations
        tm.sleep(8)


def create_paths(
    path: str,
    table_name: str,
):
    """this function creates temporary directories to store input and output files

    Args:
        path (str): a standard directory to store input and output files from all flows
        table_name (str): the name of the table to compose the directory structure and separate input and output files
        of diferent tables

    """
    path_temps = [
        path,
        path + table_name + "/input/",
        path + table_name + "/output/",
    ]

    for path_temp in path_temps:
        os.makedirs(path_temp, exist_ok=True)


def remove_latin1_accents_from_df(df):
    if not isinstance(df, pd.DataFrame):
        raise TypeError("Input must be a Pandas DataFrame.")

    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].apply(
                lambda x: "".join(
                    c
                    for c in unicodedata.normalize("NFD", str(x))
                    if unicodedata.category(c) != "Mn"
                )
            )
    return df


def get_columns_pattern_across_files(
    files_folder_path: str,
) -> dict[str, list]:
    """this function reads the first rows of all files in a folder and returns a dict
    with the file name as key and a list of column names as value

    Args:
        file_path (str): the path to the file
        file_name (str): the file name

    Returns:
        dict[str, list]: a dict with the file name as key and a list of column names as value
    """
    # read the first 20 rows of the file
    dict_columns = {}

    dir_files = os.listdir(files_folder_path)

    for file_name in dir_files:
        df = pd.read_csv(files_folder_path + "/" + file_name, nrows=20, sep=";")
        # get the column names
        cols = df.columns

        # create a dict with the file name as key and a list of column names as value
        dict_columns[file_name] = cols

    return dict_columns


def check_and_create_column(
    df: pd.DataFrame,
    col_name: List[str],
) -> pd.DataFrame:
    """
    Check if a column exists in a Pandas DataFrame. If it doesn't, create a new column with the given name
    and fill it hwith NaN values. If it does exist, do nothing.

    Parameters:
    df (pd.DataFrame): The DataFrame to check.
    col_name (List[str]): A list of column names to check and create

    Returns:
    Pandas DataFrame: The modified DataFrame.
    """
    # for each column in col_name
    for col in col_name:
        # check if the column exists in the dataframe
        if col not in df.columns:
            # if it doesnt, create a new column with the given name and fill it with "" (empty) values
            df[col_name] = ""

    return df


# ---- build rename dicts
def change_columns_name(df: pd.DataFrame, url: str) -> pd.DataFrame:
    """Essa função recebe como input uma string com link para uma tabela de arquitetura
    e retorna um dicionário com os nomes das colunas originais e os nomes das colunas
    padronizados
    Returns:
        dict: com chaves sendo os nomes originais e valores sendo os nomes padronizados
    """

    try:
        # Converte a URL de edição para um link de exportação em formato csv
        url = url.replace("edit#gid=", "export?format=csv&gid=")

        # Coloca a arquitetura em um dataframe
        df_architecture = pd.read_csv(
            StringIO(requests.get(url, timeout=10).content.decode("utf-8"))
        )

        df_architecture["original_name"] = df_architecture["original_name"].fillna("")
        df_architecture["original_name"] = df_architecture["original_name"].str.strip()
        df_architecture["name"] = df_architecture["name"].str.strip()

        values = df_architecture["name"]
        keys = df_architecture["original_name"]
        my_dict = {k: v for k, v in zip(keys, values)}

        print(my_dict)

        for key, value in my_dict.items():
            if value:
                df.rename(columns={key: value}, inplace=True)

    except Exception as e:
        # Handle any exceptions that occur during the process
        print(
            f"Algum erro ocorreu durante a renomeação das colunas usando a tabela de arquitetura: {str(e)}"
        )

    return df


def process_date_column(df: pd.DataFrame, date_column: str) -> pd.DataFrame:
    # Check if all observations are in the 'YYYY-MM-DD' format
    is_valid_format = pd.to_datetime(df[date_column], errors="coerce").notna().all()

    # Raise an ValueError if not
    if not is_valid_format:
        raise ValueError("Not all date observations are in the 'YYYY-MM-DD' format.")

    # Create year and month columns
    df["ano"] = pd.to_datetime(df[date_column]).dt.year
    df["mes"] = pd.to_datetime(df[date_column]).dt.month

    return df


def process_datetime_column(df: pd.DataFrame, datetime_column: str) -> pd.DataFrame:
    """This function creates separate columns for date and hour from a datetime column

    Args:
        df (pd.DataFrame): a datafrme with a datetime column in the format YYYY-MM-DD HH:MM:SS
        datetime_column (str): a datetime column

    Returns:
        pd.DataFrame: a dataframe with separate columns for date and hour
    """
    # Create separate columns for date and hour
    df["hora"] = df[datetime_column].str[11:19]
    df["data"] = df[datetime_column].str[0:10]

    return df


def order_df(df: pd.DataFrame, url: str) -> pd.DataFrame:
    """Essa função recebe como input uma string com link para uma tabela de arquitetura
    e retorna um dicionário com os nomes das colunas originais e os nomes das colunas
    padronizados
    Returns:
        dict: com chaves sendo os nomes originais e valores sendo os nomes padronizados
    """

    # Converte a URL de edição para um link de exportação em formato csv
    url = url.replace("edit#gid=", "export?format=csv&gid=")

    # Coloca a arquitetura em um dataframe
    df_architecture = pd.read_csv(
        StringIO(requests.get(url, timeout=10).content.decode("utf-8"))
    )

    df_architecture["name"] = df_architecture["name"].str.strip()

    list = df_architecture["name"]

    df = df[list]

    return df
