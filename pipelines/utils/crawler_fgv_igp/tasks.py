# -*- coding: utf-8 -*-
"""
Tasks for crawler_fgv_igp
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
import ipeadatapy as idpy
import pandas as pd
from prefect import task


@task
def crawler_fgv(code: str) -> pd.DataFrame:
    """
    Crawl data from ipeadata

    Args:
        code (str): the Ipeadata code

    Returns:
        pd.DataFrame: raw DataFrame from Ipeadata
    """

    return idpy.timeseries(code)


@task
def clean_fgv_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean FGV results

    Args:
        df (pd.DataFrame): the DataFrame to be cleaned

    Returns:
        pd.DataFrame: cleaned DataFrame with calculated columns
    """
    df.rename({df.columns[-1]: "indice"}, axis=1, inplace=True)
    df["indice"] = df["indice"].astype(float, errors="ignore")
    df["var_mensal"] = df["indice"].pct_change(periods=1) * 100
    df["NEXT_MONTH"] = df.shift(-1)["indice"]
    df["indice_fechamento_mensal"] = (df["indice"] * df["NEXT_MONTH"]) ** 0.5
    df.drop(columns=["DAY", "CODE", "RAW DATE", "NEXT_MONTH"], inplace=True)
    df.rename(columns={"YEAR": "ano", "MONTH": "mes"}, inplace=True)

    return df


@task
def hello_task():
    print("Hello, Test")
