# -*- coding: utf-8 -*-
"""
Tasks for br_isp_estatisticas_seguranca
"""

###############################################################################
#
# Aqui é onde devem ser definidas as tasks para os flows do projeto.
# Cada task representa um passo da pipeline. Não é estritamente necessário
# tratar todas as exceções que podem ocorrer durante a execução de uma task,
# mas é recomendável, ainda que não vá implicar em  uma quebra no sistema.
# Mais informações sobre tasks podem ser encontradas na documentação do
# Prefect: https://docs.prefect.io/core/concepts/tasks.html
#
# De modo a manter consistência na codebase, todo o código escrito passará
# pelo pylint. Todos os warnings e erros devem ser corrigidos.
#
# As tasks devem ser definidas como funções comuns ao Python, com o decorador
# @task acima. É recomendado inserir type hints para as variáveis.
#
# Um exemplo de task é o seguinte:
#
# -----------------------------------------------------------------------------
# from prefect import task
#
# @task
# def my_task(param1: str, param2: int) -> str:
#     """
#     My task description.
#     """
#     return f'{param1} {param2}'
# -----------------------------------------------------------------------------
#
# Você também pode usar pacotes Python arbitrários, como numpy, pandas, etc.
#
# -----------------------------------------------------------------------------
# from prefect import task
# import numpy as np
#
# @task
# def my_task(a: np.ndarray, b: np.ndarray) -> str:
#     """
#     My task description.
#     """
#     return np.add(a, b)
# -----------------------------------------------------------------------------
#
# Abaixo segue um código para exemplificação, que pode ser removido.
#
###############################################################################
import pandas as pd
import os
import requests
from datetime import timedelta
from typing import List
from typing import Dict

from prefect import task
from pipelines.datasets.br_rj_isp_estatisticas_seguranca.utils import (
    dict_original,
    dict_arquitetura,
    change_columns_name,
    create_columns_order,
)
from pipelines.datasets.br_rj_isp_estatisticas_seguranca.constants import (
    constants as isp_constants,
)
from pipelines.constants import constants


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def download_files(file_name: str, save_dir: str) -> str:  #

    """
    Downloads CSV files from a list of URLs and saves them to a specified directory.

    Args:
        urls (list): List of URLs to download CSV files from.
        save_dir (str): Path of directory to save the downloaded CSV files to.
    """

    # create path
    os.system(f"mkdir -p {isp_constants.OUTPUT_PATH.value}")

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

    print(f"File downloaded and saved to {file_path}")


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def clean_data(
    file: str,
    # EVOLUCAO_MENSAL_CISP.value
):

    # get file path as output from above code
    # create path and serve it as output for the task

    print(f"fazendo {file}")

    if file.endswith(".csv"):
        # todo : set files path

        df = pd.read_csv(
            isp_constants.INPUT_PATH.value + file, encoding="latin-1", sep=";"
        )
        print("arquivo lido")

    else:
        df = pd.read_excel(isp_constants.INPUT_PATH.value + file)
        print("arquivo lido")

    # find new df name
    novo_nome = dict_original()[file]

    # rename columns
    link_arquitetura = dict_arquitetura()[novo_nome]
    nomes_colunas = change_columns_name(link_arquitetura)
    df.rename(columns=nomes_colunas, inplace=True)

    print("colunas renomeadas")

    ordem_colunas = create_columns_order(link_arquitetura)

    df = df[ordem_colunas]
    print("colunas ordenadas")

    df.to_csv(
        isp_constants.OUTPUT_PATH.value + novo_nome,
        index=False,
        na_rep="",
        encoding="utf-8",
    )
    print(f"df {file} salvo com sucesso")

    return isp_constants.OUTPUT_PATH.value + novo_nome
