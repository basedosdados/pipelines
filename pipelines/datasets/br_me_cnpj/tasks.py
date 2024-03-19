# -*- coding: utf-8 -*-
"""
Tasks for br_me_cnpj
"""
import asyncio
import os
from google.cloud import storage
import pathlib
from datetime import datetime
import basedosdados as bd
from prefect import task

from pipelines.datasets.br_me_cnpj.constants import constants as constants_cnpj
from pipelines.datasets.br_me_cnpj.utils import (
    data_url,
    destino_output,
    download_unzip_csv,
    download_unzip_csv_sync,
    process_csv_empresas,
    process_csv_estabelecimentos,
    process_csv_simples,
    process_csv_socios,
)
from pipelines.utils.utils import log

ufs = constants_cnpj.UFS.value
url = constants_cnpj.URL.value
headers = constants_cnpj.HEADERS.value


@task
def calculate_defasagem():
    """
    Calculates the month lag based on the current month.

    Returns:
        int: Number of lagged months.
    """
    current_month = datetime.now().month
    current_year = datetime.now().year

    if current_year == 2023:
        if current_month >= 10:
            defasagem = 6
        else:
            defasagem = current_month - 4
    else:
        defasagem = 6

    return defasagem


@task
def get_data_source_max_date():
    """
    Checks if there are available updates for a specific dataset and table.

    Returns:
        bool: Returns True if updates are available, otherwise returns False.
    """
    # Obtém a data mais recente do site
    data_obj = data_url(url, headers).strftime("%Y-%m-%d")
    return data_obj


@task
def main(tabelas):
    """
    Performs the download, processing, and organization of CNPJ data.

    Args:
        tabelas (list): A list of tables to be processed.

    Returns:
        str: The path to the output folder where the data has been organized.
    """
    arquivos_baixados = []  # Lista para rastrear os arquivos baixados
    data_coleta = data_url(url, headers).date()  # Obtém a data da atualização dos dados

    for tabela in tabelas:
        sufixo = tabela.lower()

        # Define o caminho para a pasta de entrada (input)
        input_path = f"/tmp/data/br_me_cnpj/input/data={data_coleta}/"
        os.makedirs(input_path, exist_ok=True)
        log("Pasta destino input construído")

        # Define o caminho para a pasta de saída (output)
        output_path = destino_output(sufixo, data_coleta)

        # Loop para baixar e processar os arquivos
        for i in range(0, 10):
            if tabela != "Simples":
                nome_arquivo = f"{tabela}{i}"
                url_download = f"http://200.152.38.155/CNPJ/{tabela}{i}.zip"
                if nome_arquivo not in arquivos_baixados:
                    arquivos_baixados.append(nome_arquivo)
                    asyncio.run((download_unzip_csv(url_download, input_path)))
                    if tabela == "Estabelecimentos":
                        process_csv_estabelecimentos(
                            input_path, output_path, data_coleta, i
                        )
                    elif tabela == "Socios":
                        process_csv_socios(input_path, output_path, data_coleta, i)
                    elif tabela == "Empresas":
                        process_csv_empresas(input_path, output_path, data_coleta, i)
            else:
                nome_arquivo = f"{tabela}"
                url_download = f"https://dadosabertos.rfb.gov.br/CNPJ/{tabela}.zip"
                if nome_arquivo not in arquivos_baixados:
                    arquivos_baixados.append(nome_arquivo)
                    asyncio.run((download_unzip_csv(url_download, input_path)))
                    process_csv_simples(input_path, output_path, data_coleta, sufixo)

    return output_path

@task
def alternative_upload():
    os.makedirs("/tmp/data/backup/", exist_ok=True)
    storage_client = storage.Client()
    blobs_in_bucket = storage_client.list_blobs(
        "basedosdados-dev", prefix="staging/br_me_cnpj/socios/data=2024-03-15/"
    )
    blob_list = list(blobs_in_bucket)

    for blob in blob_list:
        savepath = "/tmp/data/backup/"

        # parse blob.name and get the csv file name
        csv_name = blob.name.split("/")[-1]

        # build folder path replicating storage hierarchy
        blob_folder = blob.name.replace(csv_name, "")

        # replicate folder hierarchy
        (pathlib.Path(savepath) / blob_folder).mkdir(parents=True, exist_ok=True)

        # download blob to savepath
        savepath = f"{savepath}/{blob.name}"
        blob.download_to_filename(filename=savepath)