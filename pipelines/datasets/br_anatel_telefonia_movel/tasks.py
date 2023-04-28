# -*- coding: utf-8 -*-
"""
Tasks for br_anatel_telefonia_movel
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

from prefect import task
import requests
import pandas as pd
import numpy as np
import os
import zipfile
from tqdm import tqdm
from utils import find_csv_files
from constants import constants


@task  # noqa
def download_txt(url: str, chunk_size: int = 128, mkdir: bool = False) -> str:
    """
    Downloads a text file from a given URL and saves it to a local directory.

    Parameters:
    -----------
    url: str
        The URL to download the text file from.
    chunk_size: int, optional
        The size of each chunk to download in bytes. Default is 128 bytes.
    mkdir: bool, optional
        Whether to create a new directory for the downloaded file. Default is False.

    Returns:
    --------
    str
        The path to the directory where the downloaded file was saved.
    """
    if mkdir:
        os.system("mkdir -p /tmp/data/input/")

    request_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36",
    }
    r = requests.get(url, headers=request_headers, stream=True, timeout=10)
    save_path = "/tmp/data/input/file.zip"
    # save_path = save_path + url.split("/")[-1]
    with open(save_path, "wb") as fd:
        for chunk in tqdm(r.iter_content(chunk_size=chunk_size)):
            fd.write(chunk)

    with zipfile.ZipFile(save_path) as z:
        z.extractall("/tmp/data/input")
    os.system('cd /tmp/data/input; find . -type f ! -iname "*.csv" -delete')

    print("Using file!")

    return "/tmp/data/input"


@task
def clean_csvs() -> pd.DataFrame:

    """
    Reads and cleans all CSV files in the '/tmp/data/input/' directory.

    Returns:
    -------
    pd.DataFrame
        The cleaned DataFrame with the following columns:
            'ano', 'mes', 'sigla_uf', 'id_municipio', 'ddd', 'cnpj', 'empresa', 'porte_empresa', 'tecnologia',
            'sinal', 'modalidade', 'pessoa', 'produto', 'acessos'
    """

    df_mun = pd.DataFrame()

    for path in tqdm(find_csv_files("/tmp/data/input/")):
        df = pd.read_csv(path, sep=";", encoding="utf-8")
        df.rename(columns=constants.RENAME.value, inplace=True)
        df.drop(["grupo_economico", "municipio", "ddd_chip"], axis=1, inplace=True)
        df["acessos_total"] = df.groupby(
            [
                "ano",
                "mes",
                "sigla_uf",
                "id_municipio",
                "ddd",
                "cnpj",
                "empresa",
                "porte_empresa",
                "tecnologia",
                "sinal",
                "modalidade",
                "pessoa",
                "produto",
            ]
        )["acessos"].transform(np.sum)
        df.sort_values(
            [
                "ano",
                "mes",
                "sigla_uf",
                "id_municipio",
                "ddd",
                "cnpj",
                "empresa",
                "porte_empresa",
                "tecnologia",
                "sinal",
                "modalidade",
                "pessoa",
                "produto",
            ],
            inplace=True,
        )
        df.drop_duplicates(
            subset=[
                "ano",
                "mes",
                "sigla_uf",
                "id_municipio",
                "ddd",
                "cnpj",
                "empresa",
                "porte_empresa",
                "tecnologia",
                "sinal",
                "modalidade",
                "pessoa",
                "produto",
            ],
            keep="first",
            inplace=True,
        )
        df.drop("acessos", axis=1, inplace=True)
        df.rename(columns={"acessos_total": "acessos"}, inplace=True)
        df = df[constants.ORDEM.value]

        df_mun = df_mun.append(df)

    df_mun["produto"] = df_mun["produto"].str.lower()
    df_mun["id_municipio"] = df_mun["id_municipio"].astype(str)
    df_mun["ddd"] = pd.to_numeric(df_mun["ddd"], downcast="integer").astype(str)
    df_mun["cnpj"] = df_mun["cnpj"].astype(str)
    df_mun = df_mun[constants.ORDEM.value]
    df_mun.sort_values(by=["ano", "mes"], inplace=True)

    return df_mun
