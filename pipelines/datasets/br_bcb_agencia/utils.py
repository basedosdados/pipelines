# -*- coding: utf-8 -*-
"""
General purpose functions for the br_bcb_agencia project
"""

###############################################################################
#
# Esse é um arquivo onde podem ser declaratas funções que serão usadas
# pelo projeto br_bcb_agencia.
#
# Por ser um arquivo opcional, pode ser removido sem prejuízo ao funcionamento
# do projeto, caos não esteja em uso.
#
# Para declarar funções, basta fazer em código Python comum, como abaixo:
#
# ```
# def foo():
#     """
#     Function foo
#     """
#     print("foo")
# ```
#
# Para usá-las, basta fazer conforme o exemplo abaixo:
#
# ```py
# from pipelines.datasets.br_bcb_agencia.utils import foo
# foo()
# ```
#
###############################################################################


import requests
from lxml import html
from io import BytesIO
from zipfile import ZipFile
from urllib.request import urlopen
import basedosdados as bd
import os
import pandas as pd
import re
import numpy as np
import unicodedata
from pipelines.utils.utils import (
    log,
)

# ---- functions to download data


def extract_download_links(url, xpath):
    """this function extract all download links from bcb agencias website

    Args:
        url (_type_): _description_
        xpath (_type_): _description_

    Returns:
        _type_: _description_
    """

    """extract all download links from bcb agencias website

    Args:
        url (str): bcb url https://www.bcb.gov.br/fis/info/agencias.asp?frame=1
        xpath (str): xpath which contais donwload links

    Returns:
        list: a list of file links
    """
    # Send a GET request to the URL
    response = requests.get(url)

    # Parse the HTML content of the response using lxml
    tree = html.fromstring(response.content)

    # Extract all the values from the given XPath
    values = tree.xpath(xpath + "/option/@value")

    return values


def download_and_unzip(url, extract_to):

    """download and unzip a zip file

    Args:
        url (str): a url
        extract_to (str): path to download data

    Returns:
        list: unziped files in a given folder
    """

    os.system(f"mkdir -p {extract_to}")

    http_response = urlopen(url)
    zipfile = ZipFile(BytesIO(http_response.read()))
    zipfile.extractall(path=extract_to)


# ---- functions to read data and make previous analysis on column names change across the years and mothns


def find_cnpj_row_number(file_path: str) -> int:
    """find the row number of the first occurrence of the CNPJ value (which is always in the first column)
    and returns the row number as an index to further read the file

    Args:
        file_path (str): the path of the files

    Returns:
        int: an index that identifies the row number of the column names
    """
    df = pd.read_excel(file_path, nrows=20)
    cnpj_row_number = df[df.iloc[:, 0] == "CNPJ"].index.tolist()[0]
    print(cnpj_row_number)
    return cnpj_row_number


# ! get_column_names deleted;
# ! count_list_values


def get_conv_names(file_path: str, skiprows: str) -> dict:
    """get column names from a file to be used as a converter

    Args:
        file_path (str): file path
        skiprows (int): Rows to skip given by find_cnpj_row_number function

    Returns:
        dict: A dict that maps the column names to the str type
    """
    df = pd.read_excel(file_path, nrows=20, skiprows=skiprows)
    cols = df.columns
    conv = dict(zip(cols, [str] * len(cols)))

    return conv


def get_files_path(folder_path: str) -> list:
    """Find files in a folder and returns a list with the files path

    Args:
        folder_path (str): a path to a folder

    Returns:
        list: list with files path
    """
    for file in os.listdir(folder_path):
        # the files format change across the year
        files_list = []
        if file.endswith(".xls") or file.endswith(".xlsx"):
            file_path = os.path.join(folder_path, file)
            files_list.append(file_path)

    return files_list


def read_file(file_path: str, file_name: str) -> pd.DataFrame:
    """Read files from a folder and returns a dataframe

    Args:
        file_path (str): a path to a file
        file_name (str): Name of the file to be read, also used as input to create_year_month_cols function

    Returns:
        pd.DataFrame: a dataframe with the file content (agencias)
    """
    try:
        # skip all rows before the row containing the CNPJ value
        skiprows = find_cnpj_row_number(file_path=file_path) + 1

        conv = get_conv_names(file_path=file_path, skiprows=skiprows)
        df = pd.read_excel(file_path, skiprows=skiprows, converters=conv, skipfooter=1)
        # todo: rethink logic. file_name param feeds another function create_year_month_cols
        df = create_year_month_cols(df=df, file=file_name)

    except Exception as e:
        log(e)
        conv = get_conv_names(file_path=file_path, skiprows=9)
        df = pd.read_excel(file_path, skiprows=9, converters=conv)
        df = create_year_month_cols(df=df, file=file_name)
    return df


def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """This function perfom general cleaning operations with DataFrame
    columns.

    Args:
        df (pd.DataFrame): a dataframe

    Returns:
        DataFrame: a dataframe with cleaned columns
    """
    # Remove leading and trailing whitespaces from column names
    df.columns = df.columns.str.strip()
    print("colnames striped")
    # Remove accents and special characters from column names
    df.columns = df.columns.map(
        lambda x: unicodedata.normalize("NFKD", x)
        .encode("ascii", "ignore")
        .decode("utf-8")
    )
    print("colnames cleaned 1")
    # Replace remaining special characters with underscores
    df.columns = df.columns.str.replace(r"[^\w\s]+", "_", regex=True)
    print("colnames cleaned 2")
    # Lowercase all column names
    df.columns = df.columns.str.lower()
    print("colnames to lower")
    return df


# ---- functions to wrang data
def create_year_month_cols(df: pd.DataFrame, file: str) -> pd.DataFrame:
    """This function creates two new columns in the dataframe,
    one for the year and another for the month.


    Args:
        df (pd.DataFrame): _description_
        file (str): it refers to the file name, which contains the year and month information.
        The year is the first 4 digits and the month is the next 2 digits.

    Returns:
        pd.DataFrame: a dataframe with month and year columns
    """
    print(f"{file}")
    df["ano"] = file[0:4]
    df["mes"] = file[4:6]
    print(f"year {file[0:4]} and month {file[4:6]} cols created")
    return df


def check_and_create_column(df: pd.DataFrame, col_name: str) -> pd.DataFrame:
    """
    Check if a column exists in a Pandas DataFrame. If it doesn't, create a new column with the given name
    and fill it with NaN values. If it does exist, do nothing.

    Parameters:
    df (Pandas DataFrame): The DataFrame to check.
    col_name (str): The name of the column to check for or create.

    Returns:
    Pandas DataFrame: The modified DataFrame.
    """
    if col_name not in df.columns:
        df[col_name] = ""
    return df


def rename_cols():
    """this function renames the columns to a standard name

    Returns:
        _type_: it returns a dict with the old and new names
    """
    rename_dict = {
        "cnpj": "cnpj",
        "sequencial do cnpj": "sequencial_cnpj",
        "dv do cnpj": "dv_do_cnpj",
        "nome instituicao": "instituicao",
        "nome da instituicao": "instituicao",
        "segmento": "segmento",
        "segmentos": "segmento",
        "cod compe ag": "id_compe_bcb_agencia",
        "cod compe bco": "id_compe_bcb_instituicao",
        "nome da agencia": "nome_agencia",
        "nome agencia": "nome_agencia",
        "endereco": "endereco",
        "numero": "numero",
        "complemento": "complemento",
        "bairro": "bairro",
        "cep": "cep",
        "municipio": "nome",
        "estado": "sigla_uf",
        "uf": "sigla_uf",
        "data inicio": "data_inicio",
        "ddd": "ddd",
        "fone": "fone",
        "id instalacao": "id_instalacao",
        "municipio ibge": "id_municipio",
    }

    return rename_dict


def order_cols():
    """Reorder columns to a standard format"""

    cols_order = [
        "ano",
        "mes",
        "sigla_uf",
        "id_municipio",
        "data_inicio",
        "cnpj",
        "nome_agencia",
        "instituicao",
        "segmento",
        "id_compe_bcb_agencia",
        "id_compe_bcb_instituicao",
        "cep",
        "endereco",
        "complemento",
        "bairro",
        "ddd",
        "fone",
        "id_instalacao",
    ]
    return cols_order


def clean_nome_municipio(df: pd.DataFrame) -> pd.DataFrame:
    """perform cleaning operations on the municipio column

    Args:
        df (pd.DataFrame): dataframe with municipio column

    Returns:
        pd.DataFrame: dataframe with municipio cleaned column
    """
    df["nome"] = df["nome"].apply(
        lambda x: unicodedata.normalize("NFKD", str(x))
        .encode("ascii", "ignore")
        .decode("utf-8")
    )
    df["nome"] = df["nome"].apply(lambda x: re.sub(r"[^\w\s]", "", x))
    df["nome"] = df["nome"].str.lower()
    df["nome"] = df["nome"].str.strip()
    return df


def read_brazilian_municipallity_ids_from_base_dos_dados(
    billing_id: str,
) -> pd.DataFrame:
    """Download municipio table from base dos dados

    Args:
        billing_id (str): BQ billing project id

    Returns:
        pd.DataFrame:  municipio table from base dos dados
    """
    municipio = bd.read_table(
        dataset_id="br_bd_diretorios_brasil",
        table_id="municipio",
        billing_project_id=billing_id,
    )
    return municipio


def remove_latin1_accents_from_df(df):
    for col in df.columns:
        df[col] = df[col].apply(
            lambda x: "".join(
                c
                for c in unicodedata.normalize("NFD", str(x))
                if unicodedata.category(c) != "Mn"
            )
        )
    return df


def remove_non_numeric_chars(s):
    """Remove non numeric chars from a pd.dataframe column"""
    return re.sub(r"\D", "", s)


def remove_empty_spaces(s):
    """Remove empty spaces from a pd.dataframe column"""
    return re.sub(r"\s", "", s)


def create_cnpj_col(df: pd.DataFrame) -> pd.DataFrame:
    """Takes an dataframe with cnpj, sequencial_cnpj and dv_do_cnpj columns
    and create a new column with the cnpj

    Args:
        df (pd.DataFrame): a dataframe with cnpj formatted column
    """
    # concat cnpj

    df["sequencial_cnpj"] = df["sequencial_cnpj"].astype(str)
    df["dv_do_cnpj"] = df["dv_do_cnpj"].astype(str)
    df["cnpj"] = df["cnpj"].astype(str)

    df["cnpj"] = df["cnpj"] + df["sequencial_cnpj"] + df["dv_do_cnpj"]
    print("cnpj built")

    return df


def str_to_title(df: pd.DataFrame, column_name: str) -> pd.DataFrame:
    df[column_name] = df[column_name].str.title()
    return df


def strip_dataframe_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Strips whitespace from all column values in a Pandas DataFrame.

    Parameters:
        df (pandas.DataFrame): The input DataFrame to be processed.

    Returns:
        pandas.DataFrame: The processed DataFrame with stripped column values.
    """
    # Use applymap to apply the strip method to all values in the DataFrame
    # Note: applymap applies a function to each element of a DataFrame
    stripped_df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    return stripped_df


def format_date(date_str):
    """Change random date format to a standard format and, if it doesnt work,
    fill the results with NaN values.
    Args:
        date_str (str): a column with dates

    Returns:
        _type_: dataframe with formated dates
    """
    try:
        date_obj = pd.to_datetime(date_str)
        formatted_date = date_obj.strftime("%Y/%m/%d")
    # ! they are random numbers in date column that breaks the code
    except ValueError:
        formatted_date = ""
    return formatted_date


def replace_nan_with_empty_set_coltypes_str(df: pd.DataFrame) -> pd.DataFrame:
    """This function replace nan values with empty strings and set all columns as strings

    Args:
        df (pd.DataFrame): a dataframe

    Returns:
        pd.DataFrame: a datrafaame with all columns as strings and nan values as empty cels
    """
    for col in df.columns:
        df[col] = df[col].astype(str)
        df[col] = df[col].replace("nan", "")
    return df


# function copied from datasets.br_tse_eleicoes.utils
def get_data_from_prod(dataset_id: str, table_id: str, columns: list) -> list:
    """
    Get select columns from a table in prod.
    """

    storage = bd.Storage(dataset_id=dataset_id, table_id=table_id)
    blobs = list(
        storage.client["storage_staging"]
        .bucket("basedosdados-dev")
        .list_blobs(prefix=f"staging/{storage.dataset_id}/{storage.table_id}/")
    )

    dfs = []

    for blob in blobs:
        partitions = re.findall(r"\w+(?==)", blob.name)
        if len(set(partitions) & set(columns)) == 0:
            df = pd.read_csv(
                blob.public_url,
                usecols=columns,
                dtype={"id_municipio": str, "id_municipio_bcb": str, "ddd": str},
            )
            dfs.append(df)
        else:
            columns2add = list(set(partitions) & set(columns))
            for column in columns2add:
                columns.remove(column)
            df = pd.read_csv(
                blob.public_url,
                usecols=columns,
                dtype={"id_municipio": str, "id_municipio_bcb": str},
            )
            for column in columns2add:
                df[column] = blob.name.split(column + "=")[1].split("/")[0]
            dfs.append(df)

    df = pd.concat(dfs)

    return df
