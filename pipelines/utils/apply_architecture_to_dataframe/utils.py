# -*- coding: utf-8 -*-
"""
General purpose functions for the process_df_with_architecture project
"""


from io import StringIO

import numpy as np
import pandas as pd
import requests

from pipelines.utils.utils import log


def apply_architecture_to_dataframe(
    df: pd.DataFrame,
    url_architecture: str,
    apply_rename_columns: bool = True,
    apply_column_order_and_selection: bool = True,
    apply_include_missing_columns: bool = True,
):
    """
    Transforms a DataFrame based on the specified architecture.

    Args:
        df (pandas DataFrame): The input DataFrame.
        url_architecture (str): The URL of the architecture.
        apply_rename_columns (bool, optional): Flag to apply column renaming. Defaults to True.
        apply_column_order_and_selection (bool, optional): Flag to apply column order and selection. Defaults to True.
        apply_include_missing_columns (bool, optional): Flag to include missing columns. Defaults to True.

    Returns:
        pandas DataFrame: The transformed DataFrame.

    Raises:
        Exception: If an error occurs during the transformation process.
    """
    architecture = read_architecture_table(url_architecture=url_architecture)

    if apply_rename_columns:
        df = rename_columns(df, architecture)

    if apply_include_missing_columns:
        df = include_missing_columns(df, architecture)

    if apply_column_order_and_selection:
        df = column_order_and_selection(df, architecture)

    return df


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

    df_architecture.query("name != '(excluido)'", inplace=True)

    return df_architecture.replace(np.nan, "", regex=True)


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
    return df.rename(columns=dict_columns)


def include_missing_columns(df, architecture):
    """
    Includes missing columns in the DataFrame based on the specified architecture.

    Args:
        df (pandas DataFrame): The input DataFrame.
        architecture (str): The specified architecture.

    Returns:
        pandas DataFrame: The modified DataFrame with missing columns included.
    """
    df_missing_columns = missing_columns(df.columns, get_order(architecture))
    if df_missing_columns:
        df[df_missing_columns] = ""
        log(f"The following columns were included into the df: {df_missing_columns}")
    else:
        log("No columns were included into the df")
    return df


def missing_columns(current_columns, specified_columns):
    """
    Determines the missing columns between the current columns and the specified columns.

    Args:
        current_columns (list): The list of current columns.
        specified_columns (list): The list of specified columns.

    Returns:
        list: The list of missing columns.
    """
    missing_columns = []
    for col in specified_columns:
        if col not in current_columns:
            missing_columns.append(col)

    return missing_columns


def column_order_and_selection(df, architecture):
    """
    Performs column order and selection on the DataFrame based on the specified architecture.

    Args:
        df (pandas DataFrame): The input DataFrame.
        architecture (str): The specified architecture.

    Returns:
        pandas DataFrame: The DataFrame with columns ordered and selected according to the architecture.
    """
    architecture_columns = get_order(architecture)
    list_missing_columns = missing_columns(
        current_columns=architecture_columns, specified_columns=df
    )
    if list_missing_columns:
        log(f"The following columns were discarded from the df: {list_missing_columns}")
    else:
        log("No columns were discarded from the df")
    return df[architecture_columns]
