# -*- coding: utf-8 -*-
"""
General purpose functions for the br-bcb-expectativa-mercado project
"""
import time as tm
import datetime
import os
import requests
import pandas as pd
from pipelines.datasets.br_bcb_expectativa_mercado.constants import (
    constants as expectativa_mercado_constants,
)
from pipelines.utils.utils import connect_to_endpoint_json, log


def create_url_month_market_expectations(date: str) -> str:
    """
    Creates parameterized url
    """
    search_url = expectativa_mercado_constants.API_URL.value[
        "expectativa_mercado_mensal"
    ].format(date)
    return search_url


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
            json_response = connect_to_endpoint_json(url)
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
