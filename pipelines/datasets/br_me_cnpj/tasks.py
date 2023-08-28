# -*- coding: utf-8 -*-
"""
Tasks for br_me_cnpj
"""
from pipelines.utils.utils import extract_last_date, log
from prefect import task
from pipelines.datasets.br_me_cnpj.constants import (
    constants as constants_cnpj,
)
from pipelines.datasets.br_me_cnpj.utils import (
    data_url,
    data_url_bd,
    destino_output,
    download_unzip_csv,
    process_csv_estabelecimentos,
    process_csv_socios,
    process_csv_empresas,
    process_csv_simples,
)
import os
import requests
import zipfile
import pandas as pd
from datetime import datetime, timedelta
from tqdm import tqdm

ufs = constants_cnpj.UFS.value
url = constants_cnpj.URL.value
url_bd = constants_cnpj.URL_BD.value
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
def check_for_updates():
    """
    Checks if there are available updates for a specific dataset and table.

    Returns:
        bool: Returns True if updates are available, otherwise returns False.
    """
    # Obtém a data mais recente do site
    data_obj = data_url(url, headers).strftime("%Y-%m-%d")

    # Obtém a última data no site BD
    data_bq_obj = data_url_bd(url_bd, headers).strftime("%Y-%m-%d")

    # Registra a data mais recente do site
    log(f"Última data no site do ME: {data_obj}")
    log(f"Última data no site da BD: {data_bq_obj}")

    # Compara as datas para verificar se há atualizações
    if data_obj > data_bq_obj:
        return True  # Há atualizações disponíveis
    else:
        return False  # Não há novas atualizações disponíveis


# @task
# def check_for_updates(dataset_id, table_id):
#     """
#     Checks if there are available updates for a specific dataset and table.

#     Args:
#         dataset_id (str): The dataset ID in BigQuery.
#         table_id (str): The table ID in BigQuery.

#     Returns:
#         bool: Returns True if updates are available, otherwise returns False.
#     """
#     # Obtém a data mais recente do site
#     data_obj = data_url(url, headers).replace(day=1).strftime("%Y-%m-%d")

#     # Obtém a última data armazenada no BigQuery e adiciona 6 meses
#     data_bq_obj = extract_last_date(
#         dataset_id, table_id, "yy-mm-dd", "basedosdados-dev"
#     ) + timedelta(days=6 * 30)
#     data_bq_obj = data_bq_obj.replace(day=1).strftime("%Y-%m-%d")

#     # Registra a data mais recente do site
#     log(f"Última data do site: {data_obj}")

#     # Compara as datas para verificar se há atualizações
#     if data_obj > data_bq_obj:
#         return True  # Há atualizações disponíveis
#     else:
#         return False  # Não há novas atualizações disponíveis


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

    tabela = tabelas[0]
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
            url_download = f"https://dadosabertos.rfb.gov.br/CNPJ/{tabela}{i}.zip"
            if nome_arquivo not in arquivos_baixados:
                arquivos_baixados.append(
                    nome_arquivo
                )  # Adiciona o nome à lista de arquivos baixados
                download_unzip_csv(url_download, input_path)
                process_csv_function = globals()[f"process_csv_{sufixo}"]
                process_csv_function(input_path, output_path, data_coleta, i)
        else:
            nome_arquivo = f"{tabela}"
            url_download = f"https://dadosabertos.rfb.gov.br/CNPJ/{tabela}.zip"
            if nome_arquivo not in arquivos_baixados:
                arquivos_baixados.append(
                    nome_arquivo
                )  # Adiciona o nome à lista de arquivos baixados
                download_unzip_csv(url_download, input_path)
                process_csv_simples(input_path, output_path, data_coleta, sufixo)

    return output_path
