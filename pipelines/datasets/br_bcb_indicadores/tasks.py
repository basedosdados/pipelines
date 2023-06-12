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

from pipelines.utils.utils import log
from prefect import task
import pandas as pd
from pipelines.datasets.br_bcb_indicadores.utils import (
    available_currencies,
    create_url,
    get_currency_data,
    read_input_csv,
    save_input,
    save_output,
    treat_df,
)


@task
def get_data(table_id: str) -> str:
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
        log(currency)
        # Retrieve data for each currency
        df = get_currency_data(currency)
        # Append the dataframe to the list
        list_dfs.append(df)

    # Concatenate all dataframes into a single dataframe
    df_final = pd.concat(list_dfs)
    log("concat all dfs")
    # Save the final dataframe to a file and obtain the full file path
    full_filepath = save_input(df_final, table_id)

    # Return the full file path
    return full_filepath


@task
def treat_data(table_id: str) -> str:
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
    df = treat_df(df, table_id)

    # Save the treated dataframe to a file and obtain the full file path
    full_filepath = save_output(df, table_id)

    # Return the full file path
    return full_filepath
