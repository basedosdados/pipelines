"""
General purpose functions for the br_ms_cnes project
"""

import datetime
import os

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
        return x.strip()
    return x


def remove_ascii_zero_from_df(df: pd.DataFrame) -> pd.DataFrame:
    """Remove ASCII 0 (NULL) de colunas tipadas como string de um DataFrame.
    Returns:
        pd.DataFrame: DataFrame sem ascii 0 (\x00).
    """
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


def parse_api_metadata(url: str | None = None) -> pd.DataFrame:
    """
    Faz uma requisição para a URL fornecida e extrai metadados de arquivos CSV.
    Args:
        url (str): A URL da API para fazer a requisição.
        headers (dict, opcional): Cabeçalhos HTTP para incluir na requisição. Padrão é None.
    Returns:
        pd.DataFrame: Um DataFrame contendo os nomes dos arquivos e suas respectivas datas de atualização.
    Raises:
        ValueError: Se a quantidade de arquivos extraídos for diferente da quantidade de datas de atualização.
    """

    soup = BeautifulSoup(requests_url(url).text, "lxml")

    csvs_com_data = []

    for x in soup.find_all("d:href"):
        href = x.text
        if not href.endswith(".csv"):
            continue

        data_raw = href.split(".")[-3]
        data_raw = data_raw.replace("D", "202")

        data = datetime.datetime.strptime(data_raw, "%Y%m%d").strftime(
            "%Y-%m-%d"
        )

        csvs_com_data.append(
            {"nome_arquivo": href.split("/")[-1], "data_atualizacao": data}
        )

    return pd.DataFrame(csvs_com_data)


def get_last_update_date(url: str) -> str:
    soup = BeautifulSoup(requests_url(url).text, "lxml")

    return str(
        max(
            datetime.datetime.strptime(
                p.find("d:getlastmodified").text, "%a, %d %b %Y %H:%M:%S GMT"
            )
            for p in soup.find_all("d:prop")
            if p.find("d:getlastmodified")
        ).date()
    )


def decide_files_to_download(
    last_update_date: str,
    df: pd.DataFrame,
    data_especifica: datetime.date | None = None,
    data_maxima: bool = True,
) -> tuple[list[str], list[datetime.datetime]]:
    """
    Decide quais arquivos baixar a depender da necessidade de atualização

    Parâmetros:
    df (pd.DataFrame): DataFrame contendo informações dos arquivos, incluindo a data de atualização e o nome do arquivo.
    data_especifica (datetime.date, opcional): Data específica para filtrar os arquivos. O Padrão é "%yyyy-%mm-%dd".
    data_maxima (bool): Se True, retorna os arquivos com a data de atualização mais recente. Padrão é True.

    Retorna:
    tuple: Uma tupla contendo uma lista de nomes de arquivos que atendem aos critérios fornecidos e a data correspondente.

    Levanta:
    ValueError: Se não houver arquivos disponíveis para a data específica fornecida.
    """

    if data_maxima:
        max_date = df["data_atualizacao"].max()
        log(
            f"A data máxima extraida da API da Receita Federal que será utilizada para comparar com os metadados da BD: {max_date}"
        )

        log(
            f"A data máxima extraida da API da Receita Federal que será utilizada para gerar partições no Storage: {last_update_date}"
        )

        return df[df["data_atualizacao"] == max_date][
            "nome_arquivo"
        ].tolist(), max_date

    elif data_especifica:
        filtered_df = df[df["data_atualizacao"] == data_especifica]
        if filtered_df.empty:
            raise ValueError(
                f"Não há arquivos disponíveis para a data {data_especifica}. Verifique o FTP da Receita Federal."
            )
        return filtered_df["nome_arquivo"].tolist(), data_especifica

    else:
        raise ValueError(
            "Critérios inválidos: deve-se selecionar pelo menos um dos parâmetros: 'data_maxima' ou 'data_especifica'."
        )


def download_csv_files(
    url: str, file_name: str, download_directory: str
) -> None:
    """
    Faz o download de um arquivo CSV a partir de uma URL e salva em um diretório especificado.

    Args:
        url (str): A URL do arquivo CSV a ser baixado.
        file_name (str): O nome do arquivo a ser salvo.
        download_directory (str): O diretório onde o arquivo será salvo.
        headers (dict): Cabeçalhos HTTP a serem enviados com a requisição.

    Returns:
        None
    """
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
        log(f"Downloaded {file_name}")
    else:
        log(
            f"Failed to download {file_name}. Status code: {response.status_code}"
        )


def preserve_zeros(x):
    """Preserva os zeros a esquerda de um número"""
    return x.strip()
