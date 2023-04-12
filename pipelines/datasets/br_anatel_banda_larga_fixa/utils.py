# -*- coding: utf-8 -*-
"""
General purpose functions for the br_anatel_banda_larga_fixa project
"""
import os
import zipfile
import requests
import pandas as pd


def download_file(url: str, download_dir: str) -> str:
    """
    Faz o download de um arquivo a partir de uma URL.

    Args:
        url (str): URL do arquivo a ser baixado.
        download_dir (str): diretório onde o arquivo será salvo.

    Returns:
        str: caminho completo do arquivo baixado.
    """
    os.makedirs(download_dir, exist_ok=True)

    response = requests.get(url)

    filename = os.path.basename(url)
    filepath = os.path.join(download_dir, filename)

    with open(filepath, "wb") as file:
        file.write(response.content)

    return filepath


def extract_file(filepath: str, extract_dir: str) -> str:
    """
    Extrai um arquivo zip para um diretório específico.
    Args:
        filepath (str): caminho do arquivo zip a ser extraído.
        extract_dir (str): diretório onde os arquivos serão extraídos.
    Returns:
        str: caminho do arquivo zip extraído.
    """
    os.makedirs(extract_dir, exist_ok=True)

    with zipfile.ZipFile(filepath, "r") as zip_ref:
        zip_ref.extractall(extract_dir)

    return os.path.join(extract_dir, os.path.basename(filepath))


def check_and_create_column(df: pd.DataFrame, col_name: str) -> pd.DataFrame:
    """
    # ! Verifique se existe uma coluna em um Pandas DataFrame. Caso contrário, crie uma nova coluna com o nome fornecido
    # ! e preenchê-lo com valores NaN. Se existir, não faça nada.

    # * Parâmetros:
    # ! df (Pandas DataFrame): O DataFrame a ser verificado.
    # ! col_name (str): O nome da coluna a ser verificada ou criada.

    # * Retorna:
    # ! Pandas DataFrame: O DataFrame modificado.
    """

    if col_name not in df.columns:
        df[col_name] = ""
    return df
