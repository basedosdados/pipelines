# -*- coding: utf-8 -*-
import pandas as pd
import os
import requests
from datetime import datetime, timedelta
from prefect import task

from pipelines.utils.utils import (
    log,
)
from pipelines.datasets.br_rj_isp_estatisticas_seguranca.utils import (
    change_columns_name,
    create_columns_order,
    check_tipo_fase,
)
from pipelines.datasets.br_rj_isp_estatisticas_seguranca.constants import (
    constants as isp_constants,
)
from pipelines.constants import constants


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def download_files(file_name: str, save_dir: str) -> str:
    """
    Downloads CSV files from a list of URLs and saves them to a specified directory.

    Args:
        urls (list): List of URLs to download CSV files from.
        save_dir (str): Path of directory to save the downloaded CSV files to.
    """

    # create path
    os.system(f"mkdir -p {isp_constants.INPUT_PATH.value}")

    # create full url
    url = isp_constants.URL.value + file_name

    print(f"Downloading file from {url}")
    # Extract the filename from the URL
    filename = os.path.basename(url)

    # Create the full path of the file
    file_path = os.path.join(save_dir, filename)

    # Send a GET request to the URL
    response = requests.get(url)

    # Raise an exception if the response was not successful
    response.raise_for_status()

    # Write the content of the response to a file
    with open(file_path, "wb") as f:
        f.write(response.content)

    log(f"File -> {file_name} downloaded and saved to {file_path}")


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def clean_data(
    file_name: str,
):
    print(f"fazendo {file_name}")

    if file_name.endswith(".csv"):
        df = pd.read_csv(
            isp_constants.INPUT_PATH.value + file_name,
            encoding="latin-1",
            sep=";",
            thousands=".",
            decimal=",",
        )
        log(f"file -> {file_name} read")

    else:
        df = pd.read_excel(
            isp_constants.INPUT_PATH.value + file_name,
            thousands=".",
            decimal=",",
        )
        log(f"file -> {file_name} read")

    # find new df name
    novo_nome = isp_constants.dict_original.value[file_name]

    log("renaming columns")
    # rename columns
    link_arquitetura = isp_constants.dict_arquitetura.value[novo_nome]
    nomes_colunas = change_columns_name(link_arquitetura)
    df.rename(columns=nomes_colunas, inplace=True)

    log("creating columns order")
    ordem_colunas = create_columns_order(nomes_colunas)

    log("checking tipo_fase col")
    df = check_tipo_fase(df)

    log("ordering columns")
    df = df[ordem_colunas]

    log("building dir")
    os.system(f"mkdir -p {isp_constants.OUTPUT_PATH.value}")

    df.to_csv(
        isp_constants.OUTPUT_PATH.value + novo_nome,
        index=False,
        na_rep="",
        encoding="utf-8",
    )
    log(f"df {file_name} salvo com sucesso")

    return isp_constants.OUTPUT_PATH.value + novo_nome

# task para retornar o ano e mes paara a atualização dos metadados.
@task
def get_today_date():
    d = datetime.now() - timedelta(days=60)

    return d.strftime("%Y-%m")
