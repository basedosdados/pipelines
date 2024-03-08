# -*- coding: utf-8 -*-
"""
General purpose functions for the br_ons_estimativa_custos project
"""

import os
import re
import time as tm
import unicodedata
from datetime import date, datetime
from io import StringIO
from typing import List, Union

import pandas as pd
import requests
import wget
from bs4 import BeautifulSoup

from pipelines.utils.utils import log


def extrai_data_recente(df: pd.DataFrame, table_name: str) -> Union[datetime, date]:
    """Essa função é utilizada durante a task wrang_data para extrair a data
    mais recente da tabela baixada pela task download_data

    Args:
        df (pd.DataFrame): O df que está sendo tratado
        table_name (str): nome da tabela (equivale ao table_id)

    Returns:
        Union[datetime, date]: Retorna um objeto datetime ou date, a depender da tabela
    """
    # dicionário que mapeia tabelas a seu formato de data
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
        df["data_hora"] = pd.to_datetime(
            df["data_hora"], format="%Y-%m-%d %H:%M:%S"
        ).dt.date
        data = df["data_hora"].max()

    if (
        date_dict[table_name] == "yyyy-mm-dd"
        and table_name == "custo_variavel_unitario_usinas_termicas"
    ):
        df["data_inicio"] = pd.to_datetime(df["data_inicio"], format="%Y-%m-%d").dt.date
        data = df["data_inicio"].max()

    if isinstance(data, pd.Timestamp):
        data = data.date()

    return data


def parse_year_or_year_month(url: str) -> datetime:
    """Extrai o ano e mês da URL de um arquivo do site do ONS

    Args:
        url (str): URL de arquivos do site do ONS

    Returns:
        datetime: retorna um objeto datetime com o ano ou ano e mês extraídos da URL
    """

    # extrai parte final da URL após o último "/"
    result = url.split("/")[-1].split(".")[-2]

    # extrai possível ano
    element1 = result.split("_")[-2]
    # extrai possível mes
    element2 = result.split("_")[-1]
    # junta ambos numa lista
    element_list = [element1, element2]
    elements = []

    # funcionamento geral->
    # checa se os elementos são ano ou mes
    # o ano é sempre representado com 4 digitos
    # o mês com 2
    # logo se o comprimento for 4 e os caracteres digítos, é um ano
    # se o comprimento for 2 e os caracteres digítos, é um mês
    # se não for nenhum dos dois, não é um ano nem um mês
    for element in element_list:
        if len(element) == 4 and re.match(r"^\d+$", element):
            # log(f"O elemento -- {element} -- é provavelmente um -- ano --")
            elements.append(element)
        elif len(element) == 2 and re.match(r"^\d+$", element):
            # log(f"O elemento -- {element} -- é provavelmente um -- mês --")
            elements.append(element)
        else:
            log(f"O elemento -- {element} -- não é um ano nem um mês")

    log(elements)
    # se a lista elements tiver comprimento 1, então é um ano
    # isto é, não foi identificado nenhum mês
    if len(elements) == 1:
        date = datetime.strptime(elements[0], "%Y")
    # se a lista elements tiver comprimento 2, então é um ano e mes
    # isto é, foi identificado um ano e mês
    elif len(elements) == 2:
        date = datetime.strptime(elements[0] + "-" + elements[1], "%Y-%m")
    # por vezes alguns arquivos aleatórios sem ano e mês são colocados no site do ONS
    # isso garante que eles não vão ser escolhidos para atualização
    # --------- Atenção: o flow vai quebrar se for usado para carregar dados históricos e existir um arquivo aleatório
    # exemplo de arquivo aleatório em 18/10/2023: Redshift_Cargaverificada-teste_titulo na tabela energia_natural_afluente https://dados.ons.org.br/dataset/ena-diario-por-reservatorio
    else:
        log(
            f"Durante a análise da URL {url} não foi possível identificar um mês ou ano. Como solução será atribuida uma data fictícia menor que a data máxima dos links corretos. Com isso, essa tabela não será selecionada para atualizar a base"
        )
        random_date = "1990-01-01"
        date = datetime.strptime(random_date, "%Y-%m-%d")

    return date


def crawler_ons(
    url: str,
) -> List[str]:
    """this function extract all download links from ONS  website
    Args:
        url (str): bcb url https://www.bcb.gov.br/fis/info/agencias.asp?frame=1

    Returns:
        list: a list of file links
    """
    response = requests.get(url)

    html = response.text
    log(f'--------html response {html}')

    soup = BeautifulSoup(html, "html.parser")

    csv_links = soup.find_all("a", href=lambda href: href and href.endswith(".csv"))

    log(f'------ csv_links {csv_links}')
    csv_urls = [link["href"] for link in csv_links]
    # Filtra valores únicos
    csv_urls = list(set(csv_urls))


    return csv_urls


def download_data(
    path: str,
    url: str,
    table_name: str,
) -> str:
    """A simple function to download data from ONS  website.

    Args:
        path (str): the path to store the data
        url (str): the table URL from ONS website.
        table_name (str): the table name
    """

    # downloads the file and saves it
    wget.download(url, out=path + table_name + "/input")
    # just for precaution,
    # sleep for 8 secs in between iterations
    tm.sleep(1)


# def download_data(
#    path: str,
#    url_list: List[str],
#    table_name: str,
# ):
#   """A simple crawler to download data from comex stat website.
#
#    Args:
#        path (str): the path to store the data
#        table_type (str): the table type is either ncm or mun. ncm stands for 'nomenclatura comum do mercosul' and
#        mun for 'município'.
#        table_name (str): the table name is the original name of the zip file with raw data from comex stat website
#    """
# selects a url given a table name
#    for url in url_list:
# log(f"Downloading data from {url}")

# downloads the file and saves it
#        wget.download(url, out=path + table_name + "/input")
#        # just for precaution,
#        # sleep for 8 secs in between iterations
#        tm.sleep(8)


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
