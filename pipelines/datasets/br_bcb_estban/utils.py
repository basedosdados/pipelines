# -*- coding: utf-8 -*-
"""
General purpose functions for the br_bcb_estban project
"""

import re

import numpy as np
import pandas as pd

from pipelines.utils.utils import log


# ---1. rename columns
def rename_columns_municipio(df: pd.DataFrame) -> pd.DataFrame:
    """this function rename columns from municipio dataframe

    Returns:
        dict: dict with old and new column names
    """
    dict = {
        "#DATA_BASE": "data_base",
        "UF": "sigla_uf",
        "CNPJ": "cnpj_basico",
        "NOME_INSTITUICAO": "instituicao",
        "AGEN_ESPERADAS": "agencias_esperadas",
        "AGEN_PROCESSADAS": "agencias_processadas",
    }

    df = df.rename(columns=dict)

    return df


def rename_columns_agencia(df: pd.DataFrame) -> pd.DataFrame:
    """this function rename columns from municipio dataframe

    Returns:
        dict: dict with old and new column names
    """
    dict = {
        "#DATA_BASE": "data_base",
        "UF": "sigla_uf",
        "CNPJ": "cnpj_basico",
        "NOME_INSTITUICAO": "instituicao",
        "AGENCIA": "cnpj_agencia",
    }

    df = df.rename(columns=dict)

    return df


def pre_cleaning_for_pivot_long_municipio(df: pd.DataFrame) -> pd.DataFrame:
    """This function drop and rename columns before the pivoting operation
    performed by the wide_to_long function.

    Args:
        df (pd.DataFrame): a dataframe estban data

    Returns:
        pd.Dataframe: _description_
    """
    df.drop(columns={"MUNICIPIO", "CODMUN_IBGE", "CODMUN"}, axis=1, inplace=True)

    df.rename(
        columns={
            "#DATA_BASE": "data_base",
            "UF": "sigla_uf",
            "CNPJ": "cnpj_basico",
            "NOME_INSTITUICAO": "instituicao",
            "AGEN_ESPERADAS": "agencias_esperadas",
            "AGEN_PROCESSADAS": "agencias_processadas",
        },
        inplace=True,
    )

    return df


def pre_cleaning_for_pivot_long_agencia(df: pd.DataFrame) -> pd.DataFrame:
    """This function drop and rename columns before the pivoting operation
    performed by the wide_to_long function.

    Args:
        df (pd.DataFrame): a dataframe estban data

    Returns:
        pd.Dataframe: _description_
    """
    df.drop(columns={"MUNICIPIO", "CODMUN_IBGE", "CODMUN"}, axis=1, inplace=True)

    df.rename(
        columns={
            "#DATA_BASE": "data_base",
            "UF": "sigla_uf",
            "CNPJ": "cnpj_basico",
            "NOME_INSTITUICAO": "instituicao",
            "AGENCIA": "cnpj_agencia",
        },
        inplace=True,
    )

    pattern = re.compile(r"(')")

    df["cnpj_agencia"] = [pattern.sub("", x) for x in df["cnpj_agencia"]]

    return df


def create_id_municipio(df: pd.DataFrame, df2: dict) -> pd.DataFrame:
    """this function creates a id_municipio column

    Args:
        df (pd.DataFrame): A ESTBAN dataset
        df2 (dict): with id_municipio_bcb as key  and id_municipio as value

    Returns:
        pd.DataFrame: Estban dataset with id_municipio column
    """

    df["id_municipio"] = df.CODMUN.map(df2)

    return df


# todo: to func: do wide para o long
def wide_to_long_municipio(df: pd.DataFrame) -> pd.DataFrame:
    """Pivot verbete columns to long format

    Args:
        df (pd.DataFrame): _description_

    Returns:
        pd.DataFrame: _description_
    """
    df = df.melt(
        id_vars=[
            "data_base",
            "sigla_uf",
            "cnpj_basico",
            "instituicao",
            "agencias_esperadas",
            "agencias_processadas",
            "id_municipio",
        ],
        var_name="verbete_descricao",
        value_name="valor",
    )

    return df


def wide_to_long_agencia(df: pd.DataFrame) -> pd.DataFrame:
    """Pivot verbete columns to long format

    Args:
        df (pd.DataFrame): _description_

    Returns:
        pd.DataFrame: _description_
    """
    df = df.melt(
        id_vars=[
            "data_base",
            "sigla_uf",
            "cnpj_basico",
            "instituicao",
            "cnpj_agencia",
            "id_municipio",
        ],
        var_name="verbete_descricao",
        value_name="valor",
    )

    return df


def condicoes(database: str, valor: float) -> None:
    # cruzado
    if database <= 198812:
        return round(valor / (1000**2 * 2750), 6)
    # cruzado novo
    elif database >= 198901 and database <= 199002:
        return round(valor / (1000 * 2750), 4)
    # cruzeiro
    elif database >= 199003 and database <= 199307:
        return round(valor / (1000 * 2750), 4)
    # cruzeiro real
    elif database > 199307 and database <= 199406:
        return round(valor / (2750), 2)
    # real
    else:
        return round(valor, 0)


def standardize_monetary_units(
    df: pd.DataFrame, date_column, value_column
) -> pd.DataFrame:
    """This function corrects monetary units from ESTBAN files.
    It relies on the data_base column being a string in the format YYYYMM,
    where YYYY is the year and MM is the month."""

    # todo: check the logic
    database = df[f"{date_column}"]
    valor = df[f"{value_column}"]

    # Calling the condicoes function from above
    condicoes_vetorizada = np.vectorize(condicoes)

    df["valor"] = condicoes_vetorizada(database, valor)  # trocar para valor

    return df


def create_id_verbete_column(df: pd.DataFrame, column_name: str) -> pd.DataFrame:
    """This function creates id_verbete column from a verbete column.
    It parses numeric digitis from the verbete column strings.

    Args:
        df (pd.DataFrame): _description_
        verbete_column (str): _description_

    Returns:
        pd.Dataframe: _description_
    """
    padrao_letras = re.compile(r"\D")

    df[column_name] = [padrao_letras.sub("", x) for x in df["verbete_descricao"]]

    return df


def create_month_year_columns(df: pd.DataFrame, date_column: str) -> pd.DataFrame:
    """This function creates month and year columns from a date column.
    It Relies on the date column being a string in the format YYYYMM,
    where YYYY is the year and MM is the month.


    Args:
        df (pd.DataFrame): a ESTBAN municipios or agencia file
        date_column (_type_): the date column from ESTBAN files being a string in the format YYYYMM

    Returns:
        pd.Dataframe: the same dataframe with the new columns
    """
    df["ano"] = df[date_column].astype(str).str.slice(0, 4)
    df["mes"] = df[date_column].astype(str).str.slice(4)

    return df


def order_cols_municipio(df: pd.DataFrame) -> pd.DataFrame:
    """this function orders the columns of the dataframe

    Returns:
        pd.DataFrame: ordered columns
    """
    order = [
        "ano",
        "mes",
        "sigla_uf",
        "id_municipio",
        "cnpj_basico",
        "instituicao",
        "agencias_esperadas",
        "agencias_processadas",
        "id_verbete",
        "valor",
    ]

    df = df[order]

    return df


def cols_order_agencia(df: pd.DataFrame) -> pd.DataFrame:
    """this function orders the columns of the dataframe

    Returns:
        pd.DataFrame: ordered columns
    """
    order = [
        "ano",
        "mes",
        "sigla_uf",
        "id_municipio",
        "cnpj_basico",
        "instituicao",
        "cnpj_agencia",
        "id_verbete",
        "valor",
    ]

    df = df[order]

    return df
