# -*- coding: utf-8 -*-
import os
import pandas as pd
import requests

from pipelines.utils.crawler_camara_dados_abertos.constants import constants as constants_camara
from pipelines.utils.utils import log

# ----------------------------------------------------------------------------------- > Universal

def download_csv(table_id: str) -> None:
    """
    Downloads CSV files from the Camara de Proposicao API.

    This function iterates over the years and table list of propositions defined in the constants module,
    and downloads the corresponding CSV files from the Camara de Proposicao API. The downloaded files are
    saved in the input path specified in the constants module.

    Raises:
        Exception: If there is an error in the request, such as a non-successful status code.

    """
    if not os.path.exists(constants_camara.INPUT_PATH.value):
        os.makedirs(constants_camara.INPUT_PATH.value)

    url = constants_camara.TABLES_URL.value[table_id]
    input_path = constants_camara.TABLES_INPUT_PATH.value[table_id]
    headers = {
        "Content-Type": "application/json;charset=UTF-8",
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko)"
    }

    log(f"Downloading {table_id} from {url}")
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        with open(input_path, "wb") as f:
            f.write(response.content)

    if response.status_code >= 400 and response.status_code <= 599:
        raise Exception(f"Error in request: {response.status_code}")


def download_and_read_data(table_id: str) -> pd.DataFrame:
    download_csv(table_id)
    input_path = constants_camara.TABLES_INPUT_PATH.value[table_id]

    df = pd.read_csv(input_path, sep=";")

    return df