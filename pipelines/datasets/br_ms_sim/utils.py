# -*- coding: utf-8 -*-
"""
General purpose functions for the br_ms_sim project
"""
import pandas as pd

def adapt_dataframe_to_columns(df: pd.DataFrame, required_columns: list[str]) -> pd.DataFrame:
    """
    Adapts the DataFrame to match the required columns.
    It ensures that all columns specified in the list exist in the DataFrame and removes any columns
    that are not part of the list.

    Parameters:
    df (pd.DataFrame): The input DataFrame to be adapted.
    required_columns (list[str]): A list of column names that the DataFrame should contain.

    Returns:
    pd.DataFrame: The adapted DataFrame.
    """
    # Add missing columns with empty values
    for col in required_columns:
        if col not in df.columns:
            df[col] = ''

    # Remove columns that are not in the required list
    columns_to_remove = [col for col in df.columns if col not in required_columns]
    df.drop(columns=columns_to_remove, inplace=True)

    return df

