# -*- coding: utf-8 -*-
import os

import pandas as pd
import requests

from pipelines.datasets.br_camara_dados_abertos.constants import constants
from pipelines.utils.apply_architecture_to_dataframe.utils import (
    apply_architecture_to_dataframe,
)
from pipelines.utils.utils import log


# -------------------------------------------------------------------------------------> VOTACAO
def download_csvs_camara_votacao() -> None:
    """
    Downloads CSV files from the Camara de Proposicao API.

    This function iterates over the years and table list of chamber defined in the constants module,
    and downloads the corresponding CSV files from the Camara de Proposicao API. The downloaded files are
    saved in the input path specified in the constants module.

    Raises:
        Exception: If there is an error in the request, such as a non-successful status code.

    """
    log("Downloading csvs from camara dos deputados")
    if not os.path.exists(constants.INPUT_PATH.value):
        os.makedirs(constants.INPUT_PATH.value)

    for chave, valor in constants.TABLE_LIST.value.items():
        log(f"download {valor}")
        url = f"http://dadosabertos.camara.leg.br/arquivos/{valor}/csv/{valor}-{constants.ANOS.value}.csv"

        response = requests.get(url)

        if response.status_code == 200:
            with open(f"{constants.INPUT_PATH.value}{valor}.csv", "wb") as f:
                f.write(response.content)
        elif response.status_code >= 400 and response.status_code <= 599:
            raise Exception(f"Erro de requisição: status code {response.status_code}")

    log("------------- archive inside in container --------------")
    log(os.listdir(constants.INPUT_PATH.value))


def get_data():
    download_csvs_camara_votacao()
    df = pd.read_csv(
        f'{constants.INPUT_PATH.value}{constants.TABLE_LIST.value["votacao_microdados"]}.csv',
        sep=";",
    )

    return df


def read_and_clean_camara_dados_abertos(
    table_id=str, path=constants.INPUT_PATH.value, date_column=str
) -> pd.DataFrame:
    """
    Lê e limpa os dados abertos da câmara.

    Esta função lê um arquivo CSV de um caminho específico, adiciona uma coluna "ano" ao DataFrame e aplica uma arquitetura específica ao DataFrame com base no ID da tabela.

    Parâmetros:
    table_id (str): ID da tabela para determinar a arquitetura a ser aplicada.
    path (str): Caminho para o arquivo CSV a ser lido.
    date_column (str): Nome da coluna que contém a data.

    Retorna:
    pd.DataFrame: DataFrame após a aplicação da arquitetura.
    """
    df = pd.read_csv(f"{path}{constants.TABLE_LIST.value[table_id]}.csv", sep=";")

    if table_id == "votacao_orientacao_bancada":
        df["ano"] = constants.ANOS.value[0]
    else:
        df["ano"] = df[date_column].str[0:4]

    log("------------- columns before apply architecture --------------")
    log(f"------------- TABLE ---------------- {table_id} --------------")
    log(df.columns)
    if table_id == "votacao_objeto":
        df.rename(columns=constants.RENAME_COLUMNS_OBJETO.value, inplace=True)
        df = df[constants.RENAME_COLUMNS_OBJETO.value.values()]

    else:
        df = apply_architecture_to_dataframe(
            df,
            url_architecture=constants.TABLE_NAME_ARCHITECTURE.value[table_id],
            apply_include_missing_columns=False,
            apply_column_order_and_selection=True,
            apply_rename_columns=True,
        )
    log("------------- columns after apply architecture --------------")
    log(f"------------- TABLE ---------------- {table_id} ------------")
    log(df.columns)
    return df


# ------------------------------------------------------------> DEPUTADOS


def download_csvs_camara_deputado() -> None:
    """
    Downloads CSV files from the Camara de Proposicao API.

    This function iterates over the years and table list of congressperson defined in the constants module,
    and downloads the corresponding CSV files from the Camara de Proposicao API. The downloaded files are
    saved in the input path specified in the constants module.

    Raises:
        Exception: If there is an error in the request, such as a non-successful status code.

    """
    log("Downloading csvs from camara dos deputados")
    if not os.path.exists(constants.INPUT_PATH.value):
        os.makedirs(constants.INPUT_PATH.value)

    for key, valor in constants.TABLE_LIST_DEPUTADOS.value.items():
        url = f"http://dadosabertos.camara.leg.br/arquivos/{valor}/csv/{valor}.csv"

        response = requests.get(url)

        if response.status_code == 200:
            with open(f"{constants.INPUT_PATH.value}{valor}.csv", "wb") as f:
                f.write(response.content)

        elif response.status_code >= 400 and response.status_code <= 599:
            raise Exception(f"Erro de requisição: status code {response.status_code}")

    log(os.listdir(constants.INPUT_PATH.value))


def read_and_clean_data_deputados(table_id):
    df = pd.read_csv(
        f"{constants.INPUT_PATH.value}{constants.TABLE_LIST_DEPUTADOS.value[table_id]}.csv",
        sep=";",
    )

    df = apply_architecture_to_dataframe(
        df,
        url_architecture=constants.TABLE_NAME_ARCHITECTURE_DEPUTADOS.value[table_id],
        apply_include_missing_columns=False,
        apply_rename_columns=True,
        apply_column_order_and_selection=True,
    )

    log(df.columns)

    return df


def get_data_deputados():
    download_csvs_camara_deputado()
    df = pd.read_csv(
        f'{constants.INPUT_PATH.value}{constants.TABLE_LIST_DEPUTADOS.value["deputado_profissao"]}.csv',
        sep=";",
    )

    return df


# ----------------------------------------------------------------------------------- > Universal


def download_csv_camara(table_id: str) -> None:
    """
    Downloads CSV files from the Camara de Proposicao API.

    This function iterates over the years and table list of propositions defined in the constants module,
    and downloads the corresponding CSV files from the Camara de Proposicao API. The downloaded files are
    saved in the input path specified in the constants module.

    Raises:
        Exception: If there is an error in the request, such as a non-successful status code.

    """
    if not os.path.exists(constants.INPUT_PATH.value):
        os.makedirs(constants.INPUT_PATH.value)

    url = constants.TABLES_URL.value[table_id]
    url_last_year = constants.TABLES_URL_LAST_BY_YEAR.value[table_id]
    input_path = constants.TABLES_INPUT_PATH.value[table_id]
    input_path_last_year = constants.TABLES_INPUT_PATH_LAST_YEAR.value[table_id]
    response = requests.get(url)
    if response.status_code == 200:
        log(f"{table_id} - {url}")
        with open(input_path, "wb") as f:
            f.write(response.content)

    else:
        response = requests.get(url_last_year)
        log(f"{table_id} - {url_last_year}")
        if response.status_code == 200:
            with open(input_path_last_year, "wb") as f:
                f.write(response.content)

    log(os.listdir(constants.INPUT_PATH.value))


def download_and_read_data(table_id: str) -> pd.DataFrame:
    download_csv_camara(table_id)

    input_path = constants.TABLES_INPUT_PATH.value[table_id]
    input_path_last_year = constants.TABLES_INPUT_PATH_LAST_YEAR.value[table_id]
    if os.path.exists(input_path):
        df = pd.read_csv(input_path, sep=";")

    else:
        df = pd.read_csv(input_path_last_year, sep=";")

    return df
