# -*- coding: utf-8 -*-
import pandas as pd
from io import StringIO
import os
import requests
from typing import List
from typing import Dict

# build a dict that maps a table name to a architectura and
# another dict that maps an original table name to a
# trated table name


# ---- build rename dicts
def change_columns_name(url_architecture: str) -> Dict[str, str]:
    """Essa função recebe como input uma string com link para uma tabela de arquitetura
    e retorna um dicionário com os nomes das colunas originais e os nomes das colunas
    padronizados

    Returns:
        dict: com chaves sendo os nomes originais e valores sendo os nomes padronizados
    """
    # Converte a URL de edição para um link de exportação em formato csv
    url = url_architecture.replace("edit#gid=", "export?format=csv&gid=")

    # Coloca a arquitetura em um dataframe
    df_architecture = pd.read_csv(
        StringIO(requests.get(url, timeout=10).content.decode("utf-8"))
    )

    # Cria um dicionário de nomes de colunas e tipos de dados a partir do dataframe df_architecture
    column_name_dict = dict(
        zip(df_architecture["original_name"], df_architecture["name"])
    )

    # Retorna o dicionário

    return column_name_dict


# ---- function to order columns


def create_columns_order(dict: Dict[str, str]) -> List[str]:
    """This function receives a dictionary with the
    original column names and the standardized column names
    and returns a list with the standardized order column names.
    from the architecture table


    Args:
        dict (Dict[str, str]): A dictionary with the original column names and the standardized column names

    Returns:
        List[str]: A list with standardized column names
    """
    ordered_list = dict.values()

    return ordered_list


# ---- function to
def check_tipo_fase(df: pd.DataFrame) -> pd.DataFrame:
    """Checks if column tipo_fase exists,
    if it does, it maps descriptions to the values

    Args:
        df (pd.DataFrame): A dataframe to check if column tipo_fase exists

    Returns: df (pd.DataFrame): A dataframe with mapped values for column tipo_fase
    """

    for col in df.columns:
        if col == "tipo_fase":
            df["tipo_fase"] = df["tipo_fase"].map(
                {
                    "2": "Consolidado sem errata",
                    "3": "Consolidado com errata",
                }
            )
        else:
            pass

    return df