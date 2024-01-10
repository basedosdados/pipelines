# -*- coding: utf-8 -*-
"""
utils for br_bd_metadata
"""
# pylint: disable=too-few-public-methods,invalid-name
import os
import pandas as pd


def save_input(df: pd.DataFrame, table_id: str) -> str:
    """
    Saves a DataFrame as a CSV file in the input folder.

    Args:
        df (pd.DataFrame): The DataFrame to be saved.
        table_id (str): The identifier for the table.

    Returns:
        str: The full file path of the saved CSV file.

    """


    return None