# -*- coding: utf-8 -*-
import os
from io import BytesIO
from urllib.request import urlopen
from zipfile import ZipFile

import pandas as pd
import requests

from pipelines.utils.crawler_camara_dados_abertos.constants import (
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
    
    for url_year, input_path_year in dict(zip(url, input_path)).items():
        log(
            f"Downloading {table_id} from {url_year} and extracting to {input_path_year}"
        )
        log("1, 2, 3 - Testando!!!")
        http_response = urlopen(url_year)
        zipfile = ZipFile(BytesIO(http_response.read()))
        zipfile.extractall(path=input_path_year)

        


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

    for url_year, input_path_year in dict(zip(url, input_path)).items():
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
            raise Exception(f"Error in request: {e}")


def download_and_read_data(table_id: str) -> pd.DataFrame:
    for input_path in [
        constants_camara.TABLES_INPUT_PATH.value[table_id],
        constants_camara.TABLES_INPUT_PATH_ANO_ANTERIOR.value[table_id],
    ]:
        if table_id == "despesa":
            download_table_despesa(table_id)
        else:
            download_all_table(table_id)
        log(
            f"Reading {table_id} from {input_path} and extracting to {input_path}"
        )
        df = pd.read_csv(input_path, sep=";")

    return df
