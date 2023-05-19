# -*- coding: utf-8 -*-
"""
General purpose functions for the br_cvm_fi project
"""
from io import StringIO
import requests
import pandas as pd
import os
import re
from unidecode import unidecode


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


def obter_anos_meses(diretorio):
    """
    Retorna uma lista com todos os arquivos AAAAMM presentes no diretório.
    """
    lista_arquivos = os.listdir(diretorio)
    padrao_AAAAMM = re.compile(r"cda_fi_BLC_\d+_(\d{6}).csv")

    anos_meses = set()
    for arquivo in lista_arquivos:
        match = padrao_AAAAMM.match(arquivo)
        if match:
            ano_mes = match.group(1)
            anos_meses.add(ano_mes)

    return list(anos_meses)


def limpar_string(texto):
    texto = str(texto)
    # Remover acentos
    texto = unidecode(texto)
    # Remover pontuações
    texto = re.sub(r"[^\w\s]", "", texto)
    # Converter para letras minúsculas
    texto = texto.lower()
    return texto


def check_and_create_column(df: pd.DataFrame, colunas_totais: list) -> pd.DataFrame:
    """
    Check if a column exists in a Pandas DataFrame. If it doesn't, create a new column with the given name
    and fill it with NaN values. If it does exist, do nothing.

    Parameters:
    df (Pandas DataFrame): The DataFrame to check.
    col_name (str): The name of the column to check for or create.

    Returns:
    Pandas DataFrame: The modified DataFrame.
    """
    for col_name in colunas_totais:
        if col_name not in df.columns:
            df[col_name] = ""
    return df
