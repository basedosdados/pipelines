# -*- coding: utf-8 -*-
"""
utils for br_bd_inndicadores
"""
# pylint: disable=too-few-public-methods,invalid-name
from datetime import datetime

import pandas as pd


def flatten_list(array):
    """
    Flattens a list of lists into one list of strings.
    """
    return [str(item) for sublist in array for item in sublist]


def classify_frequency(interval):
    """
    Classify a temporal coverage interval's frequency as: year, month, day, other
    """

    if "(" in interval and ")" in interval:
        initial = interval.split("(")[0]
        final = interval.split(")")[1]
    else:
        initial = interval
        final = interval

    initial_dashes = initial.count("-")
    final_dashes = final.count("-")

    if max(initial_dashes, final_dashes) == 2:
        frequency = "day"
    elif max(initial_dashes, final_dashes) == 1:
        frequency = "month"
    elif max(initial_dashes, final_dashes) == 0:
        frequency = "year"
    else:
        frequency = "other"

    return frequency


def get_temporal_coverage_elements(interval):
    """
    Return the three temporal coverage interval's elements: start, end, delta
    """

    if "(" in interval and ")" in interval:
        start = interval.split("(")[0]
        end = interval.split(")")[1]
        delta = int(interval[interval.find("(") + 1 : interval.find(")")])
    else:
        start = interval
        end = interval
        delta = 0

    return start, end, delta


def get_interval_range(start, end, delta, frequency):
    """
    Return a range list based on an interval's elements
    """

    if delta > 0:
        if frequency == "year":
            sub = [*range(int(start), int(end) + 1, delta)]
        elif frequency == "month":
            start_date = datetime.strptime(start, "%Y-%m")
            end_date = datetime.strptime(end, "%Y-%m")
            date_range = pd.period_range(start_date, end_date, freq="M")
            sub = [str(date) for date in date_range]
        elif frequency == "day":
            start_date = datetime.strptime(start, "%Y-%m-%d")
            end_date = datetime.strptime(end, "%Y-%m-%d")
            date_range = pd.period_range(start_date, end_date, freq="D")
            sub = [str(date) for date in date_range]
        else:
            pass
    else:
        sub = [start]

    return sub


def get_temporal_coverage_list(temporal_coverage_field: list) -> list:
    """
    Generates a list of dates covered by resource in format YYYY[-MM][-DD].
    """

    temporal_coverage_list = []

    for s in temporal_coverage_field:
        frequency = classify_frequency(s)
        start, end, delta = get_temporal_coverage_elements(s)
        sub = get_interval_range(start, end, delta, frequency)
        temporal_coverage_list.append(sub)

    return flatten_list(temporal_coverage_list)


def check_missing_metadata(df: pd.DataFrame) -> pd.DataFrame:
    """
    Verifica se há valores faltando em cada coluna de um DataFrame e armazena essa informação em uma nova coluna.

    Args:
        df (pandas.DataFrame): O DataFrame a ser verificado.

    Returns:
        pandas.DataFrame: O DataFrame original com uma nova coluna chamada 'missing_metadata', que contém valores booleanos indicando se há valores nulos em cada linha.
    """
    # Cria uma nova coluna com valor padrão 'False'
    df["missing_metadata"] = False

    # Verifica se há valores nulos em cada coluna
    for col in df.loc[:, df.columns != "outdated"].columns:
        if df[col].isnull().values.any():
            # Se houver valores nulos, atualiza a coluna 'missing_metadata' para 'True'
            df["missing_metadata"] = True

    # Retorna o DataFrame atualizado
    return df
