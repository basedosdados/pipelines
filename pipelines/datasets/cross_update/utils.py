# -*- coding: utf-8 -*-
"""
Utils for cross_update pipeline
"""
import os

import pandas as pd

from pipelines.utils.utils import log


def save_file(df: pd.DataFrame, table_id: str) -> str:
    """
    Saves a DataFrame as a CSV file.

    Args:
        df (pd.DataFrame): The DataFrame to be saved.
        table_id (str): The identifier for the table.

    Returns:
        str: The full file path of the saved CSV file.

    """

    # Define the folder path for storing the file
    folder = f"tmp/{table_id}"
    # Create the folder if it doesn't exist
    os.system(f"mkdir -p {folder}")
    # Define the full file path for the CSV file
    full_filepath = f"{folder}/{table_id}.csv"
    # Save the DataFrame as a CSV file
    df.to_csv(full_filepath, index=False)
    log("save_input")
    return full_filepath
