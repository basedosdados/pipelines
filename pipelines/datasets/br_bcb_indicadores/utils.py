# -*- coding: utf-8 -*-
"""
General purpose functions for the br_bcb_indicadores project
"""
import requests
import pandas as pd
import datetime
import pytz
from datetime import timedelta
import os
from io import StringIO
import numpy as np
import time as tm
from pipelines.datasets.br_bcb_indicadores.constants import constants as bcb_constants
from pipelines.utils.utils import log, to_partitions

###############################################################################
#
# Esse é um arquivo onde podem ser declaratas funções que serão usadas
# pelo projeto br_bcb_indicadores.
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
# from pipelines.datasets.br_bcb_indicadores.utils import foo
# foo()
# ```
#
###############################################################################


def available_currencies() -> dict:
    """
    Return available currencies from api
    """
    url = bcb_constants.API_URL.value["taxa_cambio_moedas"]
    json_response = connect_to_endpoint(url)
    log(f"available currencies:{json_response['value']}")
    return json_response["value"]


def create_url_currency(start_date: str, end_date: str, moeda="USD") -> str:
    """
    Creates parameterized url
    """
    search_url = bcb_constants.API_URL.value["taxa_cambio"].format(
        moeda, start_date, end_date
    )
    log(search_url)
    return search_url


def create_url_selic(start_date: str, end_date: str, moeda="USD") -> str:
    """
    Creates parameterized url
    """
    search_url = bcb_constants.API_URL.value["taxa_selic"].format(
        moeda, start_date, end_date
    )
    log(search_url)
    return search_url


def create_url_month_market_expectations(date: str) -> str:
    """
    Creates parameterized url
    """
    search_url = bcb_constants.API_URL.value["expectativa_mercado_mensal"].format(date)
    return search_url


def connect_to_endpoint(url: str) -> dict:
    """
    Connect to endpoint
    """
    response = requests.request("GET", url, timeout=30)
    log("Endpoint Response Code: " + str(response.status_code))
    if response.status_code != 200:
        log(Exception(response.status_code, response.text))
    return response.json()


def get_currency_data(currency: dict) -> pd.DataFrame:
    """
    Retrieves currency data for a specific currency from an API endpoint.

    Args:
        currency (dict): A dictionary containing information about the currency.

    Returns:
        pd.DataFrame: A Pandas DataFrame containing the retrieved currency data.

    Raises:
        requests.exceptions.Timeout: If the connection to the API endpoint times out.

    """

    # Get the start date as 14 days before the current date
    start_day = datetime.now(tz=pytz.UTC) - timedelta(days=14)
    start_day = start_day.strftime("%m-%d-%Y")

    # Log the start day
    log(f"start day: {start_day}")

    # Calculate the end date as 7 days before the current date
    now = datetime.now(tz=pytz.UTC) - timedelta(days=7)
    end_day = now.strftime("%m-%d-%Y")

    # Log the end day
    log(f"end day: {end_day}")

    # Get the currency code for the current currency
    currency_code = currency["simbolo"]

    # Create the URL for retrieving data using the start and end dates and currency code
    log("creating_url")
    url = create_url_currency(start_day, end_day, currency_code)

    # Connect to the API endpoint and retrieve the JSON response
    attempts = 0
    while attempts < 3:
        attempts += 1
        log(f"tentativa {attempts}")
        try:
            log("connecting to endpoint")
            json_response = connect_to_endpoint(url)
            break
        except requests.exceptions.Timeout:
            tm.sleep(3)

    # Extract the currency data from the JSON response
    data = json_response["value"]

    # Convert the data into a DataFrame
    log("transform json into df")
    df = pd.DataFrame(data)

    # Add columns about the currency code to the DataFrame
    log("Add columns about the currency code")
    df["moeda"] = currency["simbolo"]
    df["tipo_moeda"] = currency["tipoMoeda"]

    return df


def get_selic_data() -> pd.DataFrame:
    """
    Retrieves currency data for a specific currency from an API endpoint.

    Returns:
        pd.DataFrame: A Pandas DataFrame containing the retrieved currency data.

    Raises:
        requests.exceptions.Timeout: If the connection to the API endpoint times out.

    """
    # Create the URL for retrieving data using the start and end dates and currency code
    log("creating_url")
    url = bcb_constants.API_URL.value["taxa_selic"]

    # Connect to the API endpoint and retrieve the JSON response
    attempts = 0
    while attempts < 3:
        attempts += 1
        log(f"tentativa {attempts}")
        try:
            log("connecting to endpoint")
            json_response = connect_to_endpoint(url)
            break
        except requests.exceptions.Timeout:
            tm.sleep(3)

    # Extract the currency data from the JSON response
    data = json_response

    # Convert the data into a DataFrame
    log("transform json into df")
    df = pd.DataFrame(data)

    return df


def get_market_expectations_data_day(date: datetime.date) -> pd.DataFrame:
    """
    Retrieves market expectations data for a specific date from an API endpoint.

    Args:
        date (datetime.date): The date for which to retrieve market expectations data.

    Returns:
        pd.DataFrame: A Pandas DataFrame containing the retrieved market expectations data.

    Raises:
        requests.exceptions.Timeout: If the connection to the API endpoint times out.

    """

    # Convert the date to a string in the format "YYYY-MM-DD"
    date_str = date.strftime("%Y-%m-%d")

    # Create the URL for retrieving data using the date
    url = create_url_month_market_expectations(date_str)

    # Log the information about the date and URL
    log(f"Downloading data for {date_str}")
    log(url)

    # Connect to the API endpoint and retrieve the JSON response
    attempts = 0
    while attempts < 3:
        attempts += 1
        log(f"Attempt {attempts}")
        try:
            log("Connecting to endpoint")
            json_response = connect_to_endpoint(url)
            break
        except requests.exceptions.Timeout:
            tm.sleep(3)
    log("Download task completed successfully!")

    # Extract the market expectations data from the JSON response
    data = json_response["value"]

    # Convert the data into a DataFrame
    log("Transforming JSON into DataFrame")
    df = pd.DataFrame(data)
    return df


def treat_currency_df(df: pd.DataFrame, table_id: str) -> pd.DataFrame:
    """
    Performs data treatment on the currency DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame containing currency data.
        table_id (str): The identifier for the architecture table.

    Returns:
        pd.DataFrame: The treated DataFrame with renamed and reordered columns.

    """

    # Convert the 'dataHoraCotacao' column in 'df' to datetime format
    log("Convert the 'dataHoraCotacao' column in 'df' to datetime format")
    df["dataHoraCotacao"] = pd.to_datetime(
        df["dataHoraCotacao"], format="%Y-%m-%d %H:%M:%S.%f"
    )

    # Extract the date and time components from 'dataHoraCotacao' column and create new columns 'data_cotacao' and 'hora_cotacao'
    log(
        "Extract the date and time components from 'dataHoraCotacao' column and create new columns 'data_cotacao' and 'hora_cotacao'"
    )
    df["data_cotacao"] = df["dataHoraCotacao"].dt.date
    df["hora_cotacao"] = df["dataHoraCotacao"].dt.time

    # Read the architecture table from the given URL and store it in the 'architecture' variable
    log(
        "Read the architecture table from the given URL and store it in the 'architecture' variable"
    )
    architecture = read_architecture_table(
        url_architecture=bcb_constants.ARCHITECTURE_URL.value[table_id]
    )

    # Rename the columns of the DataFrame 'df' based on the architecture table
    log("Rename the columns of the DataFrame 'df' based on the architecture table")
    df = rename_columns(df, architecture)

    # Get the column order from the architecture table
    log("Get the column order from the architecture table")
    order = get_order(architecture=architecture)

    # Reorder the columns of 'df' based on the specified order
    log("Reorder the columns of 'df' based on the specified order")
    return df[order]


def treat_selic_df(df: pd.DataFrame, table_id: str) -> pd.DataFrame:
    """
    Performs data treatment on the currency DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame containing data.
        table_id (str): The identifier for the architecture table.

    Returns:
        pd.DataFrame: The treated DataFrame with renamed and reordered columns.

    """

    log("Convert the 'data' column in 'df' to date format")
    df["data"] = pd.to_datetime(df["data"], format="%d/%m/%Y").dt.date

    return df


def treat_market_expectations_df(df: pd.DataFrame, table_id: str) -> pd.DataFrame:
    """
    Performs data treatment on a market expectations DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame containing market expectations data.
        table_id (str): The identifier for the architecture table.

    Returns:
        pd.DataFrame: The treated DataFrame with renamed and reordered columns.

    """
    log("Convert the 'DataReferencia' column in 'df' to datetime format")
    df["DataReferencia"] = pd.to_datetime(df["DataReferencia"], format="%m/%Y")

    log(
        "Extract the date and time components from 'dataHoraCotacao' column and create new columns 'data_cotacao' and 'hora_cotacao'"
    )
    df["DataReferencia"] = df["DataReferencia"].dt.date

    log(
        "Read the architecture table from the given URL and store it in the 'architecture' variable"
    )
    architecture = read_architecture_table(
        url_architecture=bcb_constants.ARCHITECTURE_URL.value[table_id]
    )

    log("Rename the columns of the DataFrame 'df' based on the architecture table")
    df = rename_columns(df, architecture)

    log("Get the column order from the architecture table")
    order = get_order(architecture=architecture)

    log("Reorder the columns of 'df' based on the specified order")
    return df[order]


def read_architecture_table(url_architecture: str) -> pd.DataFrame:
    """URL contendo a tabela de arquitetura no formato da base dos dados

    Args:
        url_architecture (str): url de tabela de arquitera no padrão da base dos dados

    Returns:
        df: um df com a tabela de arquitetura
    """
    # Converte a URL de edição para um link de exportação em formato csv
    url = url_architecture.replace("edit#gid=", "export?format=csv&gid=")

    # Coloca a arquitetura em um dataframe
    df_architecture = pd.read_csv(
        StringIO(requests.get(url, timeout=10).content.decode("utf-8"))
    )

    log("Download of the architecture table")
    return df_architecture.replace(np.nan, "", regex=True)


def rename_columns(df: pd.DataFrame, architecture: pd.DataFrame) -> pd.DataFrame:
    """
    Renames the columns of a DataFrame based on an architecture table.

    Args:
        df (pd.DataFrame): The DataFrame to rename columns.
        architecture (pd.DataFrame): The architecture table containing column mappings.

    Returns:
        pd.DataFrame: The DataFrame with renamed columns.

    """

    # Create a DataFrame 'aux' with unique mappings of column names from the architecture table
    aux = architecture[["name", "original_name"]].drop_duplicates(
        subset=["original_name"], keep=False
    )

    # Create a dictionary 'dict_columns' with column name mappings
    dict_columns = dict(zip(aux.original_name, aux.name))

    # Rename columns of the DataFrame 'df' based on the dictionary 'dict_columns'
    log("Rename columns")
    return df.rename(columns=dict_columns)


def get_order(architecture: pd.DataFrame) -> list:
    """
    Retrieves the column order from an architecture table.

    Args:
        architecture (pd.DataFrame): The architecture table containing column information.

    Returns:
        list: The list of column names representing the order.

    """

    # Return the list of column names from the 'name' column of the architecture table
    return list(architecture["name"])


def save_input(df: pd.DataFrame, table_id: str) -> str:
    """
    Saves a DataFrame as a CSV file in the input folder.

    Args:
        df (pd.DataFrame): The DataFrame to be saved.
        table_id (str): The identifier for the table.

    Returns:
        str: The full file path of the saved CSV file.

    """

    # Define the folder path for storing the file
    folder = f"tmp/{table_id}/input/"
    # Create the folder if it doesn't exist
    os.system(f"mkdir -p {folder}")
    # Define the full file path for the CSV file
    full_filepath = f"{folder}/{table_id}.csv"
    # Save the DataFrame as a CSV file
    df.to_csv(full_filepath, index=False)
    log("save_input")
    return full_filepath


def save_output(df: pd.DataFrame, table_id: str) -> str:
    """
    Saves a DataFrame as a CSV file in the output folder.

    Args:
        df (pd.DataFrame): The DataFrame to be saved.
        table_id (str): The identifier for the table.

    Returns:
        str: The full file path of the saved CSV file.

    """

    # Define the folder path for storing the file
    folder = f"tmp/{table_id}/output/"
    # Create the folder if it doesn't exist
    os.system(f"mkdir -p {folder}")
    # Define the full file path for the CSV file
    full_filepath = f"{folder}/{table_id}.csv"
    # Save the DataFrame as a CSV file
    df.to_csv(full_filepath, index=False)
    log("save_output")
    return full_filepath


def read_input_csv(table_id: str):
    """
    Reads and returns the input CSV file as a DataFrame.

    Args:
        table_id (str): The identifier for the table.

    Returns:
        pd.DataFrame: The DataFrame read from the input CSV file.

    """

    # Define the path of the input CSV file
    path = f"tmp/{table_id}/input/{table_id}.csv"
    log("read input")
    # Read the CSV file as a DataFrame
    return pd.read_csv(path)
