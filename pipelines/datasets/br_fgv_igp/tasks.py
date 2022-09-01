# -*- coding: utf-8 -*-
"""
Tasks for br_fgv_igp
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
import csv
import pathlib

import ipeadatapy as idpy
import numpy as np
import pandas as pd
from prefect import task


@task  # noqa
def crawler_fgv(code: str) -> pd.DataFrame:
    """
    Crawl data from ipeadata

    Args:
        code (str): the Ipeadata code

    Returns:
        pd.DataFrame: raw DataFrame from Ipeadata
    """

    return idpy.timeseries(code)


@task  # noqa
def clean_fgv_df(
    df: pd.DataFrame, root: pathlib.PosixPath, period: str = "mes"
) -> pathlib.PosixPath:
    """
    Clean FGV results

    Args:
        df (pd.DataFrame): the DataFrame to be cleaned
        root: (pathlib.Path): where is the root for tmp folder for data
        period (str): the period of the time series [mensal|anual]

    Returns:
        str: the path of the csv file from DataFrame
    """
    if period not in ["mes", "ano"]:
        raise Exception("Period must be 'mes' or 'ano'")

    filepath = root / f"igpdi_{period}.csv"

    if not root.is_dir():
        root.mkdir(parents=True, exist_ok=False)

    var_period = f"var_{period}"
    var_end_period = f"indice_fechamento_{period}"

    # absolute value with aug/1994 = 100
    df.rename({df.columns[-1]: "indice"}, axis=1, inplace=True)

    # monthly variation in percent
    df["indice"] = df["indice"].astype(float, errors="ignore")
    df[var_period] = df["indice"].pct_change(periods=1) * 100
    df["NEXT_PERIOD"] = df.shift(-1)["indice"]

    # end of period: Ipeadata geometric mean between current and next month
    df[var_end_period] = (df["indice"] * df["NEXT_PERIOD"]) ** 0.5

    # drop and rename columns
    df.drop(columns=["DAY", "CODE", "RAW DATE", "NEXT_PERIOD"], inplace=True)
    df.rename(columns={"YEAR": "ano", "MONTH": "mes"}, inplace=True)

    df.reset_index(drop=True, inplace=True)

    df.to_csv(
        filepath,
        encoding="utf-8",
        sep=",",
        decimal=".",
        na_rep=np.nan,
        quoting=csv.QUOTE_NONNUMERIC,
        index=False,
        header=True,
    )

    return filepath
