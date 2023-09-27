# -*- coding: utf-8 -*-
"""
Tasks for br-bcb-taxa-selic
"""
from prefect import task

from pipelines.datasets.br_bcb_taxa_selic.utils import (
    get_selic_data,
    read_input_csv,
    save_input,
    save_output,
    treat_selic_df,
)


@task
def get_data_taxa_selic(table_id: str) -> str:
    """
    Retrieves data from an API for multiple currencies, concatenates the resulting dataframes,
    saves the final dataframe to a file, and returns the full file path.

    Args:
        table_id (str): The identifier for the table.

    Returns:
        str: The full file path where the data is saved.
    """

    df = get_selic_data()

    full_filepath = save_input(df, table_id)
    print(f"input saved in {full_filepath}")

    # Return the full file path
    return full_filepath


@task
def treat_data_taxa_selic(table_id: str) -> str:
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
    df = treat_selic_df(df, table_id)
    max_date = df["data"].max()

    # Save the treated dataframe to a file and obtain the full file path
    full_filepath = save_output(df, table_id)

    file_info = {
        "save_output_path": full_filepath,
        "max_date": max_date.strftime("%Y-%m-%d"),
    }

    # Return the full file path
    return file_info
