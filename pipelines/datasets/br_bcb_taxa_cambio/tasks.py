# -*- coding: utf-8 -*-
"""
Tasks for br_bcb_indicadores
"""

import pandas as pd
from prefect import task

from pipelines.datasets.br_bcb_taxa_cambio.constants import constants as bcb_constants
from pipelines.datasets.br_bcb_taxa_cambio.utils import (
    available_currencies,
    get_currency_data,
    read_input_csv,
    save_input,
    treat_currency_df,
)
from pipelines.utils.utils import log, to_partitions


@task
def get_data_taxa_cambio(table_id: str) -> str:
    """
    Retrieves data from an API for multiple currencies, concatenates the resulting dataframes,
    saves the final dataframe to a file, and returns the full file path.

    Args:
        table_id (str): The identifier for the table.

    Returns:
        str: The full file path where the data is saved.
    """

    # List to store individual currency dataframes
    list_dfs = []

    # Iterate over available currencies
    for currency in available_currencies():
        log(f"downloading data for {currency['simbolo']}")
        # Retrieve data for each currency
        df = get_currency_data(currency)
        log("download task successfully !")

        # Append the dataframe to the list
        list_dfs.append(df)

    # Concatenate all dataframes into a single dataframe
    df_final = pd.concat(list_dfs)
    log("all dfs concated")
    # Save the final dataframe to a file and obtain the full file path
    full_filepath = save_input(df_final, table_id)
    log(f"input saved in {full_filepath}")

    # Return the full file path
    return full_filepath


@task
def treat_data_taxa_cambio(table_id: str) -> str:
    """
    Reads input data from a CSV file, performs data treatment on the dataframe,
    saves the treated dataframe to a file, and returns the full file path.

    Args:
        table_id (str): The identifier for the table.

    Returns:
        str: The full file path where the treated data is saved.
    """

    # Read input data from a CSV file
    df = read_input_csv(table_id)

    # Perform data treatment on the dataframe
    df = treat_currency_df(df, table_id)
    max_date = df["data_cotacao"].max()
    log(max_date)

    save_output_path = f"tmp/{table_id}/output/"

    to_partitions(data=df, partition_columns=["ano"], savepath=save_output_path)

    file_info = {
        "save_output_path": save_output_path,
        "max_date": max_date.strftime("%Y-%m-%d"),
    }

    # Return the full file path
    return file_info
