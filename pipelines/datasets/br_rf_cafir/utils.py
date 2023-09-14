# -*- coding: utf-8 -*-
"""
General purpose functions for the br_ms_cnes project
"""

from bs4 import BeautifulSoup
import requests
from datetime import datetime
import os
from pipelines.utils.utils import log
import unicodedata
import pandas as pd
import basedosdados as bd

# função para extrair datas
# valor usado para o check de atualização do site além de ser usado para update do coverage
# tambem será usado para criar uma coluna


def extract_last_date(
    dataset_id: str, table_id: str, billing_project_id: str
) -> datetime:
    """
    Extracts the last update date of a given dataset table.

    Args:
        dataset_id (str): The ID of the dataset.
        table_id (str): The ID of the table.
        billing_project_id (str): The billing project ID.

    Returns:
        str: The last update date in the format 'yyyy-mm-dd'.

    Raises:
        Exception: If an error occurs while extracting the last update date.
    """

    query_bd = f"""
    SELECT MAX(data_referencia) as max_date
    FROM
    `{billing_project_id}.{dataset_id}.{table_id}`
    """

    t = bd.read_sql(
        query=query_bd,
        billing_project_id=billing_project_id,
        from_file=True,
    )

    data = t["max_date"][0]

    log(f"A data mais recente da tabela é: {data}")

    return data


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
        lambda x: x.encode("ascii", "ignore").decode("ascii")
        if isinstance(x, str)
        else x
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


def parse_date_parse_files(url: str) -> tuple[list[datetime], list[str]]:
    """Faz o parse da data de atualização e dos links de download dos arquivos

    Args:
        url (string): A url do ftp do CAFIR da receita federal

    Returns:
        tuple[list[datetime],list[str]]: Retorna uma tupla com duas listas. A primeira contém uma lista de datas de atualização dos dados e a segunda contém uma lista com os nomes dos arquivos.
    """

    xpath_release_date = "tr td:nth-of-type(3)"
    response = requests.get(url)

    # Checa se a requisição foi bem sucedida
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, "html.parser")

        ## 1. parsing da data de atualização ##

        # Seleciona tags com base no xpath
        td_elements = soup.select(xpath_release_date)
        # Extrai texto das tags selecionadas
        td_texts = [td.get_text(strip=True) for td in td_elements]
        # selecionar someente data
        td_texts = [td[0:10] for td in td_texts]
        # seleciona data
        # são liberados 20 arquivos a cada atualização, a data é a mesma o que varia é a hora.
        td_texts = td_texts[3]
        # converte para data
        release_data = datetime.strptime(td_texts, "%Y-%m-%d").date()
        log(f"realease date: {release_data}")

        ## 2. parsing dos nomes dos arquivos ##

        a_elements = soup.find_all("a")
        # Extrai todos href
        href_values = [a["href"] for a in a_elements]
        # Filtra href com nomes dos arquivos
        files_name = [href for href in href_values if "PARTE" in href]
        log(f"files name: {files_name}")
        return release_data, files_name

    else:
        log(
            f"Não foi possível acessar o site :/. O status da requisição foi: {response.status_code}"
        )
        return []


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
