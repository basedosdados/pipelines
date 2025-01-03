# -*- coding: utf-8 -*-
"""
General purpose functions for the br_ms_cnes project
"""

import os
import unicodedata
from datetime import datetime
import time

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


def parse_date_parse_files(url: str, retries: int = 3, backoff_factor: int = 2) -> Tuple[datetime, List[str]]:
    """
    Faz o parse da data de atualização e dos links de download dos arquivos.

    Args:
        url (str): A URL do FTP do CAFIR da Receita Federal.
        retries (int): Número de tentativas em caso de falha por timeout.
        backoff_factor (int): Fator de espera exponencial entre tentativas.

    Returns:
        Tuple[datetime, List[str]]: Retorna uma tupla com a data de atualização e os nomes dos arquivos.
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "pt-BR,pt;q=0.8,en-US;q=0.5,en;q=0.3",
        "Sec-GPC": "1",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-User": "?1",
        "Priority": "u=0, i",
    }

    xpath_release_date = "tr td:nth-of-type(3)"
    attempt = 0

    while attempt < retries:
        try:
            response = requests.get(url, headers=headers, timeout=(10, 30))

            # Checa se a requisição foi bem-sucedida
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, "html.parser")

                # 1. Parsing da data de atualização
                td_elements = soup.select(xpath_release_date)
                td_texts = [td.get_text(strip=True)[:10] for td in td_elements]
                release_data = datetime.strptime(td_texts[3], "%Y-%m-%d").date()
                log(f"Release date: {release_data}")

                # 2. Parsing dos nomes dos arquivos
                a_elements = soup.find_all("a")
                href_values = [a["href"] for a in a_elements if a.has_attr("href")]
                files_name = [href for href in href_values if "csv" in href]
                log(f"Files name: {files_name}")

                return release_data, files_name

            else:
                log(f"Erro na requisição: {response.status_code}")
                raise requests.RequestException(f"HTTP {response.status_code}")

        except (requests.Timeout, requests.ConnectionError) as e:
            attempt += 1
            wait_time = backoff_factor ** attempt
            log(f"Erro: {e}. Tentando novamente em {wait_time} segundos...")
            time.sleep(wait_time)

    raise TimeoutError("Falha após várias tentativas de conectar ao servidor.")



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
