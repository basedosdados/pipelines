# -*- coding: utf-8 -*-
"""
General purpose functions for the br-bcb-taxa-selic project
"""

from io import BytesIO
import os
from urllib.request import urlopen
from zipfile import ZipFile
import pandas as pd
import requests
import time as tm
from pipelines.utils.utils import (
    log,
)
from pipelines.utils.apply_architecture_to_dataframe.utils import (
    apply_architecture_to_dataframe,
)
from pipelines.datasets.br_bcb_taxa_selic.constants import (
    constants as taxa_selic_constants,
)


def create_url_selic(start_date: str, end_date: str, moeda="USD") -> str:
    """
    Creates parameterized url
    """
    search_url = taxa_selic_constants.API_URL.value["taxa_selic"].format(
        moeda, start_date, end_date
    )
    log(search_url)
    return search_url


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
    url = taxa_selic_constants.API_URL.value["taxa_selic"]

    # Connect to the API endpoint and retrieve the JSON response
    attempts = 0
    while attempts < 3:
        attempts += 1
        log(f"tentativa {attempts}")
        try:
            log("connecting to endpoint")
            json_response = connect_to_endpoint_json(url)
            break
        except requests.exceptions.Timeout:
            tm.sleep(3)

    # Extract the currency data from the JSON response
    data = json_response

    # Convert the data into a DataFrame
    log("transform json into df")
    df = pd.DataFrame(data)

    return df


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

    df = apply_architecture_to_dataframe(
        df=df,
        url_architecture=taxa_selic_constants.ARCHITECTURE_URL.value[
            "expectativa_mercado_mensal"
        ],
        rename_columns=True,
        set_order=True,
        adjust_data_types=False,
    )


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

def download_and_unzip(url, path):
    """download and unzip a zip file
    Args:
        url (str): a url
    Returns:
        list: unziped files in a given folder
    """

    os.system(f"mkdir -p {path}")

    http_response = urlopen(url)
    zipfile = ZipFile(BytesIO(http_response.read()))
    zipfile.extractall(path=path)

    return path


def connect_to_endpoint_json(url: str, max_attempts: int = 3) -> dict:
    """
    Connect to endpoint
    """
    attempts = 0

    while attempts < max_attempts:
        attempts += 1
        response = requests.request("GET", url, timeout=30)
        log("Endpoint Response Code: " + str(response.status_code))
        if response.status_code != 200:
            log(Exception(response.status_code, response.text))
            break
    return response.json()