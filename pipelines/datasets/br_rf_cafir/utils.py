# -*- coding: utf-8 -*-
"""
General purpose functions for the br_ms_cnes project
"""

import os
import unicodedata
from datetime import datetime
import time
from typing import Tuple
import pandas as pd
import requests
from bs4 import BeautifulSoup

from pipelines.utils.utils import log
from typing import Tuple, List


def strip_string(x: pd.DataFrame) -> pd.DataFrame:
    """Aplica o strip em uma coluna de um dataframe, caso seja string.
    ps: usar com applymap

    Args:
        x (pd.Dataframe): Dataframe

    Returns:
        pd.Dataframe: Dataframe com valores de linha sem espaços no início e no final das strings
    """
    if isinstance(x, str):
        return x.strip()
    return x


def remove_non_ascii_from_df(df: pd.DataFrame) -> pd.DataFrame:
    """Remove caracteres não ascii de um dataframe codificando a coluna e decodificando em seguida

    Returns:
        pd.DataFrame: Um dataframe
    """

    return df.applymap(
        lambda x: (
            x.encode("ascii", "ignore").decode("ascii") if isinstance(x, str) else x
        )
    )


def remove_accent(input_str: pd.DataFrame, pattern: str = "all") -> pd.DataFrame:
    """Remove acentos e caracteres especiais do encoding LATIN-1 para a coluna selecionada
    #creditos para -> https://stackoverflow.com/questions/39148759/remove-accents-from-a-dataframe-column-in-r

    Returns:
        pd.Dataframe: Dataframe com coluna sem acentos e caracteres especiais
    """

    if not isinstance(input_str, str):
        input_str = str(input_str)

    patterns = set(pattern)

    if "Ç" in patterns:
        patterns.remove("Ç")
        patterns.add("ç")

    symbols = {
        "acute": "áéíóúÁÉÍÓÚýÝ",
        "grave": "àèìòùÀÈÌÒÙ",
        "circunflex": "âêîôûÂÊÎÔÛ",
        "tilde": "ãõÃÕñÑ",
        "umlaut": "äëïöüÄËÏÖÜÿ",
        "cedil": "çÇ",
    }

    nude_symbols = {
        "acute": "aeiouAEIOUyY",
        "grave": "aeiouAEIOU",
        "circunflex": "aeiouAEIOU",
        "tilde": "aoAOnN",
        "umlaut": "aeiouAEIOUy",
        "cedil": "cC",
    }

    accent_types = ["´", "`", "^", "~", "¨", "ç"]

    if any(
        pattern in {"all", "al", "a", "todos", "t", "to", "tod", "todo"}
        for pattern in patterns
    ):
        return "".join(
            [
                c if unicodedata.category(c) != "Mn" else ""
                for c in unicodedata.normalize("NFD", input_str)
            ]
        )

    for p in patterns:
        if p in accent_types:
            input_str = "".join(
                [
                    c if c not in symbols[p] else nude_symbols[p][symbols[p].index(c)]
                    for c in input_str
                ]
            )

    return input_str


def parse_api_metadata(url: str, headers: dict = None) -> pd.DataFrame:

    log(f'Fazendo request para a url: {url}')

    response = requests.get(url, headers=headers)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    elementos = soup.find_all("a")

    log('Extraindo nomes de arquivos e datas de atualização')

    linhas_arquivos_datas =  [arquivo.find_parent('td') for arquivo in elementos if arquivo.has_attr('href') and 'csv' in arquivo['href']]
    nomes_arquivos =  [arquivo.find('a').get('href') for arquivo in linhas_arquivos_datas]

    data_atualizacao_arquivos = [data_atualizacao.find_next_sibling('td') for data_atualizacao in linhas_arquivos_datas if data_atualizacao.find_next_sibling('td').get('align') == 'right']
    data_atualizacao_arquivos_formatada = [datetime.strptime(a.text.strip(), "%Y-%m-%d %H:%M").date() for a in data_atualizacao_arquivos]


    if len(nomes_arquivos) != len(data_atualizacao_arquivos_formatada):
        raise ValueError(
            f"A quantidade de arquivos ({len(nomes_arquivos)}) difere da quantidade de datas ({len(data_atualizacao_arquivos_formatada)}). Verifique o FTP da Receita Federal {url}"
        )

    df =  pd.DataFrame(
        {
        'nome_arquivo':nomes_arquivos,
        'data_atualizacao':data_atualizacao_arquivos_formatada
        }
    )
    log(df)
    log('Extração finalizada')
    return df

def decide_files_to_download(df: pd.DataFrame, data_especifica: datetime.date = None, data_maxima: bool = True) -> tuple[list[str],list[datetime]]:
    """
    Decide quais arquivos baixar a depender da necessidade de atualização

    Parâmetros:
    df (pd.DataFrame): DataFrame contendo informações dos arquivos, incluindo a data de atualização e o nome do arquivo.
    data_especifica (datetime.date, opcional): Data específica para filtrar os arquivos.
    data_maxima (bool): Se True, retorna os arquivos com a data de atualização mais recente. Padrão é True.

    Retorna:
    tuple: Uma tupla contendo uma lista de nomes de arquivos que atendem aos critérios fornecidos e a data correspondente.

    Levanta:
    ValueError: Se não houver arquivos disponíveis para a data específica fornecida.
    """
    #TODO: Aqui caberia um check da qte de arquivos seleciondos. As vezes alguns arquiivos são atualizados depois de outrs
    if data_maxima:
        max_date = df['data_atualizacao'].max()
        log(f"Os arquivos serão selecionados utiliozando a data de atualização mais recente: {max_date}")
        return df[df['data_atualizacao'] == max_date]['nome_arquivo'].tolist(), max_date

    elif data_especifica:
        filtered_df = df[df['data_atualizacao'] == data_especifica]
        if filtered_df.empty:
            raise ValueError(f"Não há arquivos disponíveis para a data {data_especifica}. Verifique o FTP da Receita Federal.")
        return filtered_df['nome_arquivo'].tolist(), data_especifica

    else:
        raise ValueError("Critérios inválidos: deve-se selecionar pelo menos um dos parâmetros 'data_maxima', 'dados_historicos' ou 'data_especifica'.")



def download_csv_files(url, file_name, download_directory):
    # cria diretório
    os.makedirs(download_directory, exist_ok=True)

    log(f"Downloading--------- {url}")
    # Extrai links de download

    # Setta path
    file_path = os.path.join(download_directory, file_name)

    # faz request
    response = requests.get(url)

    if response.status_code == 200:
        # Salva no diretório especificado
        with open(file_path, "wb") as f:
            f.write(response.content)
        print(f"Downloaded {file_name}")
    else:
        print(f"Failed to download {file_name}. Status code: {response.status_code}")


def preserve_zeros(x):
    """Preserva os zeros a esquerda de um número"""
    return x.strip()
