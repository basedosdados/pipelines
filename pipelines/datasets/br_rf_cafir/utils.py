"""
General purpose functions for the br_ms_cnes project
"""

import datetime
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

from pipelines.utils.utils import log


def strip_string(x: pd.DataFrame) -> pd.DataFrame:
    """Aplica o strip em uma coluna de um dataframe, caso seja string.
    ps: usar com applymap

    Args:
        x (pd.Dataframe): Dataframe

    Returns:
        pd.Dataframe: Dataframe com valores de linha sem espaços no início e no final das strings
    """
    if isinstance(x, str):
        # pyrefly: ignore [bad-return]
        return x.strip()
    return x


def remove_ascii_zero_from_df(df: pd.DataFrame) -> pd.DataFrame:
    """Remove ASCII 0 (NULL) de colunas tipadas como string de um DataFrame.
    Returns:
        pd.DataFrame: DataFrame sem ascii 0 (\x00).
    """
    # pyrefly: ignore [not-callable]
    return df.applymap(
        lambda x: x.replace("\x00", "") if isinstance(x, str) else x
    )


def requests_url(url: str) -> requests.Response:
    xml_body = """<?xml version="1.0" encoding="utf-8" ?>
    <d:propfind xmlns:d="DAV:">
      <d:allprop/>
    </d:propfind>
    """

    headers = {
        "Depth": "1",
        "Content-Type": "application/xml",
        "Accept": "application/xml",
        "User-Agent": "Mozilla/5.0",
    }
    try:
        response = requests.request(
            method="PROPFIND",
            url=url,
            headers=headers,
            data=xml_body,
            timeout=30,
        )

        response.raise_for_status()

    except requests.exceptions.RequestException as e:
        log(f"Erro durante a requisição: {e}")
        raise

    return response


def parse_api_metadata(response_text: str) -> pd.DataFrame:
    """
    Extrai metadados de arquivos CSV a partir da resposta da API.
    Args:
        response_text (str): Texto da resposta da API a ser parseado.
    Returns:
        pd.DataFrame: Um DataFrame contendo os nomes dos arquivos e suas respectivas datas de atualização.
    Raises:
        ValueError: Se a quantidade de arquivos extraídos for diferente da quantidade de datas de atualização.
    """
    soup = BeautifulSoup(response_text, "lxml")

    items_urls = soup.find_all("d:href")
    items_dates = soup.find_all("d:prop")

    files_metadata = []
    for index, item in enumerate(items_urls):
        href = item.text
        if href.endswith(".csv"):
            reference_date_str = href.split(".")[-3].replace("D", "202")
            reference_date = datetime.datetime.strptime(
                reference_date_str, "%Y%m%d"
            ).strftime("%Y-%m-%d")
            update_date = datetime.datetime.strptime(
                items_dates[index].find("d:getlastmodified").text,
                "%a, %d %b %Y %H:%M:%S GMT",
            )

            files_metadata.append(
                {
                    "nome_arquivo": href.split("/")[-1],
                    "data_referencia": reference_date,
                    "data_modificacao": update_date,
                }
            )
    return pd.DataFrame(files_metadata)


def get_last_date(df_metadata: pd.DataFrame, date_column: str) -> str:
    max_date = df_metadata[date_column].max()
    return str(max_date.date())


def download_csv_files(url: str, file_name: str, input_folder: Path) -> None:
    """
    Faz o download de um arquivo CSV a partir de uma URL e salva em um diretório especificado.

    Args:
        url (str): A URL do arquivo CSV a ser baixado.
        file_name (str): O nome do arquivo a ser salvo.
        input_folder (Path): O diretório onde o arquivo será salvo.
        headers (dict): Cabeçalhos HTTP a serem enviados com a requisição.

    Returns:
        None
    """
    log(f"Downloading--------- {url}")
    file_path = input_folder / file_name
    response = requests.get(url)

    if response.status_code == 200:
        with open(file_path, "wb") as f:
            f.write(response.content)
        log(f"Downloaded {file_name}")
    else:
        log(
            f"Failed to download {file_name}. Status code: {response.status_code}"
        )


def preserve_zeros(x):
    """Preserva os zeros a esquerda de um número"""
    return x.strip()
