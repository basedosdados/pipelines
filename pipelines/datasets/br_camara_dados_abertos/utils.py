import os
from io import BytesIO
from urllib.request import urlopen
from zipfile import ZipFile

import pandas as pd
import requests

from pipelines.datasets.br_camara_dados_abertos.constants import (
    constants as constants_camara,
)
from pipelines.utils.utils import log


# ----------------------------------------------------------------------------------- > Universal
def download_table_despesa(table_id: str) -> None:
    url = [
        constants_camara.TABLES_URL.value[table_id],
        constants_camara.TABLES_URL_ANO_ANTERIOR.value[table_id],
    ]
    input_path = [
        constants_camara.INPUT_PATH.value,
        constants_camara.INPUT_PATH.value,
    ]

    for url_year, input_path_year in dict(
        zip(url, input_path, strict=False)
    ).items():
        try:
            log(
                f"Downloading {table_id} from {url_year} and extracting to {input_path_year}"
            )
            http_response = urlopen(url_year)
            zipfile = ZipFile(BytesIO(http_response.read()))
            zipfile.extractall(path=input_path_year)
            log(f"File downloaded successfully to {input_path_year}")
        except requests.exceptions.RequestException as e:
            raise Exception(f"Error in request: {e}") from e


def download_all_table(table_id: str) -> None:
    """
    Downloads CSV files from the Camara de Proposicao API.

    This function iterates over the years and table list of propositions defined in the constants module,
    and downloads the corresponding CSV files from the Camara de Proposicao API. The downloaded files are
    saved in the input path specified in the constants module.

    Raises:
        Exception: If there is an error in the request, such as a non-successful status code.

    """

    url = [
        constants_camara.TABLES_URL.value[table_id],
        constants_camara.TABLES_URL_ANO_ANTERIOR.value[table_id],
    ]
    input_path = [
        constants_camara.TABLES_INPUT_PATH.value[table_id],
        constants_camara.TABLES_INPUT_PATH_ANO_ANTERIOR.value[table_id],
    ]

    for url_year, input_path_year in dict(
        zip(url, input_path, strict=False)
    ).items():
        os.makedirs(constants_camara.INPUT_PATH.value, exist_ok=True)

        log(
            f"Downloading {table_id} from {url_year} and extracting to {input_path_year}"
        )
        try:
            response = requests.get(
                url_year, headers=constants_camara.HEADERS.value, timeout=10
            )
            response.raise_for_status()
            with open(input_path_year, "wb") as f:
                f.write(response.content)

            log(f"File downloaded successfully to {input_path_year}")
        except requests.exceptions.RequestException as e:
            raise Exception(f"Error in request: {e}") from e


def check_historico_cobre_ano_atual(historico_path: str) -> None:
    """falha se alguma linha do arquivo do ano atual não está no de 1999."""
    url = constants_camara.TABLES_URL.value["licitacao_contrato"]
    response = requests.get(
        url, headers=constants_camara.HEADERS.value, timeout=60
    )
    if response.status_code != 200:
        log(f"Arquivo do ano atual indisponível, checagem pulada: {url}")
        return
    ano_atual = pd.read_csv(BytesIO(response.content), sep=";", dtype=str)
    historico = pd.read_csv(historico_path, sep=";", dtype=str)
    faltantes = ano_atual.merge(
        historico.drop_duplicates(), how="left", indicator=True
    ).query("_merge == 'left_only'")
    if not faltantes.empty:
        raise ValueError(
            f"{len(faltantes)} linhas de {url} não estão no arquivo de 1999"
        )
    log(f"Arquivo de 1999 contém as {len(ano_atual)} linhas do ano atual")


def download_licitacao_contrato() -> list[pd.DataFrame]:
    """baixa só o arquivo de 1999, que traz todos os contratos."""
    url = constants_camara.LICITACAO_CONTRATO_URL.value
    input_path = constants_camara.LICITACAO_CONTRATO_INPUT_PATH.value
    os.makedirs(constants_camara.INPUT_PATH.value, exist_ok=True)
    log(f"Downloading licitacao_contrato from {url}")
    response = requests.get(
        url, headers=constants_camara.HEADERS.value, timeout=60
    )
    response.raise_for_status()
    with open(input_path, "wb") as f:
        f.write(response.content)
    check_historico_cobre_ano_atual(input_path)
    return [pd.read_csv(input_path, sep=";")]


def download_and_read_data(table_id: str) -> list[pd.DataFrame]:
    if table_id == "licitacao_contrato":
        return download_licitacao_contrato()
    if table_id == "despesa":
        download_table_despesa(table_id)
    else:
        download_all_table(table_id)
    df_year = []
    for input_path in [
        constants_camara.TABLES_INPUT_PATH.value[table_id],
        constants_camara.TABLES_INPUT_PATH_ANO_ANTERIOR.value[table_id],
    ]:
        log(
            f"Reading {table_id} from {input_path} and extracting to {input_path}"
        )
        df = pd.read_csv(input_path, sep=";")
        df_year.append(df)

    return df_year
