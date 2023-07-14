# -*- coding: utf-8 -*-
"""
Tasks for br-bcb-expectativa-mercado
"""


import datetime
import pandas as pd
from prefect import task
from pipelines.datasets.br_bcb_taxa_selic.utils import treat_market_expectations_df
from pipelines.utils.utils import log, to_partitions
from pipelines.datasets.br_bcb_expectativa_mercado.utils import (
    get_market_expectations_data_day,
    read_input_csv,
    save_input,
)


@task
def get_data_expectativa_mercado_mensal(days_to_run):
    """
    Retrieves market expectations data for a specific number of days and saves it to a file.

    Args:
        days_to_run (int): The number of days for which to retrieve market expectations data.

    Returns:
        str: The filepath where the data is saved.

    """
    # Get current date
    current_date = datetime.date.today()
    start_day = current_date - datetime.timedelta(days_to_run)

    # List to store individual dates dataframes
    list_dfs = []

    for delta in range(days_to_run + 1):
        date = start_day + datetime.timedelta(delta)
        df = get_market_expectations_data_day(date)

        log("Append the dataframe to the list")
        list_dfs.append(df)

    df_final = pd.concat(list_dfs)
    log("all dfs concated")
    filepath = save_input(df_final, table_id="expectativa_mercado_mensal")

    # Return the full file path
    return filepath


@task
def treat_data_expectativa_mercado_mensal(table_id: str) -> str:
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
    log("df.head(1)")
    log(df.head(1))
    # Perform data treatment on the dataframe

    log("Starting treatment")
    df = treat_market_expectations_df(df, table_id)

    # Save the treated dataframe to a file and obtain the full file path
    folder = f"tmp/{table_id}/output/"
    log(folder)
    to_partitions(data=df, partition_columns=["data_calculo"], savepath=folder)

    # Return the full file path
    return folder
