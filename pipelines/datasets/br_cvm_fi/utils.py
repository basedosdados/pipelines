# -*- coding: utf-8 -*-
"""
General purpose functions for the br_cvm_fi project
"""
from io import StringIO
import requests
import pandas as pd


def sheet_to_df(columns_config_url_or_path):
    """
    Convert sheet to dataframe. Check if your google sheet Share are: Anyone on the internet with this link can view
    """
    url = columns_config_url_or_path.replace("edit#gid=", "export?format=csv&gid=")
    return pd.read_csv(StringIO(requests.get(url, timeout=10).content.decode("utf-8")))


def rename_columns(df_origem, df_destino):
    """
    Rename DataFrame columns based on architecture
    """
    # Seleciona as colunas "name" e "original_name" do dataframe de origem
    colunas = df_origem[["name", "original_name"]]

    # Cria um dicionário que mapeia as colunas do dataframe de origem para as colunas do dataframe de destino
    mapeamento_colunas = dict(zip(colunas["original_name"], colunas["name"]))

    # Renomeia as colunas do dataframe de destino com base no dicionário de mapeamento
    df_destino = df_destino.rename(columns=mapeamento_colunas)

    # Retorna o dataframe de destino com as colunas renomeadas
    return df_destino
