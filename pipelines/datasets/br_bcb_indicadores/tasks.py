# -*- coding: utf-8 -*-
"""
Tasks for br_bcb_indicadores
"""

###############################################################################
#
# Aqui é onde devem ser definidas as tasks para os flows do projeto.
# Cada task representa um passo da pipeline. Não é estritamente necessário
# tratar todas as exceções que podem ocorrer durante a execução de uma task,
# mas é recomendável, ainda que não vá implicar em  uma quebra no sistema.
# Mais informações sobre tasks podem ser encontradas na documentação do
# Prefect: https://docs.prefect.io/core/concepts/tasks.html
#
# De modo a manter consistência na codebase, todo o código escrito passará
# pelo pylint. Todos os warnings e erros devem ser corrigidos.
#
# As tasks devem ser definidas como funções comuns ao Python, com o decorador
# @task acima. É recomendado inserir type hints para as variáveis.
#
# Um exemplo de task é o seguinte:
#
# -----------------------------------------------------------------------------
# from prefect import task
#
# @task
# def my_task(param1: str, param2: int) -> str:
#     """
#     My task description.
#     """
#     return f'{param1} {param2}'
# -----------------------------------------------------------------------------
#
# Você também pode usar pacotes Python arbitrários, como numpy, pandas, etc.
#
# -----------------------------------------------------------------------------
# from prefect import task
# import numpy as np
#
# @task
# def my_task(a: np.ndarray, b: np.ndarray) -> str:
#     """
#     My task description.
#     """
#     return np.add(a, b)
# -----------------------------------------------------------------------------
#
# Abaixo segue um código para exemplificação, que pode ser removido.
#
###############################################################################

import datetime
from pipelines.utils.utils import log, to_partitions
from prefect import task
import pandas as pd
from pipelines.datasets.br_bcb_indicadores.utils import (
    available_currencies,
    get_currency_data,
    get_market_expectations_data_day,
    get_selic_data,
    read_input_csv,
    save_input,
    save_output,
    treat_currency_df,
    treat_market_expectations_df,
    treat_selic_df,
)


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

    # Save the treated dataframe to a file and obtain the full file path
    full_filepath = save_output(df, table_id)

    # Return the full file path
    return full_filepath


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

    # Save the treated dataframe to a file and obtain the full file path
    full_filepath = save_output(df, table_id)

    # Return the full file path
    return full_filepath


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
