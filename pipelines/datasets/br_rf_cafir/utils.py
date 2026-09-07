"""
General purpose functions for the br_ms_cnes project
"""

import datetime
import os
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter

from pipelines.datasets.br_rf_cafir.constants import (
    constants as br_rf_cafir_constants,
)
from pipelines.utils.utils import log

# Sessão compartilhada entre downloads paralelos: pool por host, em vez de abrir uma conexão nova a cada arquivo.
_session = requests.Session()
_adapter = HTTPAdapter(pool_connections=10, pool_maxsize=10, max_retries=3)
_session.mount("https://", _adapter)
_session.mount("http://", _adapter)


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
            )
            # .strftime("%Y-%m-%d")
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

    Usa uma sessão HTTP compartilhada (pool de conexões) e streaming para não
    carregar o arquivo inteiro na memória antes de gravar em disco.

    Args:
        url (str): A URL do arquivo CSV a ser baixado.
        file_name (str): O nome do arquivo a ser salvo.
        input_folder (Path): O diretório onde o arquivo será salvo.

    Returns:
        None

    Raises:
        requests.exceptions.RequestException: Se o download falhar.
    """
    log(f"Downloading--------- {url}")
    file_path = input_folder / file_name

    with _session.get(
        url,
        headers=br_rf_cafir_constants.HEADERS.value,
        stream=True,
        timeout=60,
    ) as response:
        response.raise_for_status()
        with open(file_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                f.write(chunk)

    log(f"Downloaded {file_name}")


def preserve_zeros(x):
    """Preserva os zeros a esquerda de um número"""
    return x.strip()


def process_csv_file(
    file_path: Path,
    reference_date: datetime.date,
    last_modified_date: datetime.date,
    output_folder: Path,
) -> Path:
    """
    Lê, limpa e particiona um único arquivo csv de largura fixa da Receita
    Federal (CAFIR). Usada tanto sequencialmente quanto em paralelo.

    Args:
        file_path (Path): Caminho do arquivo baixado a ser processado.
        reference_date (date): Data de referência do arquivo.
        last_modified_date (date): Data da última modificação do arquivo no servidor da Receita.
        output_folder (Path): Pasta raiz onde as partições serão salvas.

    Returns:
        Path: Caminho do arquivo csv processado e salvo.
    """
    file_name = file_path.name
    log(f"Lendo arquivo: {file_name} de : {file_path}")

    df = pd.read_fwf(
        file_path,
        widths=br_rf_cafir_constants.WIDTHS.value,
        names=br_rf_cafir_constants.COLUMN_NAMES.value,
        dtype=br_rf_cafir_constants.DTYPE.value,
        converters={
            col: preserve_zeros
            for col in br_rf_cafir_constants.COLUMN_NAMES.value
        },
        encoding="ISO-8859-1",
    )

    # Remove ascii /x00 (zero) - pois dá erro na materialização no BQ
    df = remove_ascii_zero_from_df(df)

    # Tira os espacos em branco
    # pyrefly: ignore [not-callable]
    df = df.applymap(strip_string)
    df["data_modificacao"] = last_modified_date
    log(f"Salvando arquivo: {file_name}")

    # NOTE: Com modificação do formato de divulgação do FTP os arquivos passaram a ser divulgados csvs particionados por UF
    # A partir de 2025, a nomenclaruta dos no Storage arquivos mudou para: "imoveis_rurais_uf_numero.csv" no lugar de "imoveris_rurais_numero.csv"
    partitions_path = (
        output_folder
        / "imoveis_rurais"
        / f"data={reference_date.strftime('%Y-%m-%d')}"
    )
    partitions_path.mkdir(parents=True, exist_ok=True)

    save_path = partitions_path / (
        "imoveis_rurais_" + file_name.split(".")[-2] + ".csv"
    )

    df.to_csv(
        save_path,
        index=False,
        sep=",",
        na_rep="",
        encoding="utf-8",
        escapechar="\\",
    )

    log(f"Arquivo salvo: {save_path.as_posix().split('/')[-1]}")

    del df
    os.remove(file_path)

    return save_path
