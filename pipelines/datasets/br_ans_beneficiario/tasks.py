# -*- coding: utf-8 -*-
"""
Tasks for br_ans_beneficiario
"""

import asyncio
import os
import re
from datetime import datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup
from prefect import task
from tqdm import tqdm

from pipelines.datasets.br_ans_beneficiario.utils import (
    get_url_from_template,
    parquet_partition,
)
from pipelines.utils.to_download.utils import download_files_async
from pipelines.utils.utils import log


@task
def extract_links_and_dates(url) -> pd.DataFrame:
    """
    Extracts all file names and their respective last update dates in a pandas dataframe.
    """

    response = requests.get(url)

    if response.status_code != 200:
        raise requests.HTTPError(
            f"Erro HTTP: A resposta da API malsucedida. O código retornado foi:  {response.status_code}"
        )

    soup = BeautifulSoup(response.content, "html.parser")

    # Encontra todos os links dentro do HTML
    links = soup.find_all("a")
    links
    links_zip = []
    for link in links:
        if link.has_attr("href") and link["href"].startswith("20"):
            links_zip.append(link["href"].replace("/", ""))

    # Encontra todas as datas de atualização dentro do HTML
    padrao = r"\d{4}-\w{2}-\d{2} \d{2}:\d{2}"
    datas = soup.find_all(string=lambda text: re.findall(padrao, text))
    datas_atualizacao = []

    for data in datas:
        data_atualizacao = re.findall(padrao, data)[0]
        datas_atualizacao.append(data_atualizacao)

    dados = {
        "arquivo": links_zip,
        "ultima_atualizacao": datas_atualizacao[:-1],
        "data_hoje": datetime.now().strftime("%Y-%m-%d"),
    }

    df = pd.DataFrame(dados)
    df.ultima_atualizacao = df.ultima_atualizacao.apply(
        lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M").strftime("%Y-%m-%d")
    )

    df["desatualizado"] = df["data_hoje"] == df["ultima_atualizacao"]
    # df['desatualizado'] = df['arquivo'].apply(lambda x: True if x in ['inf_diario_fi_202201.zip','inf_diario_fi_202305.zip'] else False)
    return df


@task
def check_if_update_date_is_today(df):
    if max(df["data_hoje"]) == max(df["ultima_atualizacao"]):
        return True
    else:
        return False


@task
def get_file_max_date(df):
    lista = df[
        df["ultima_atualizacao"] == max(df["ultima_atualizacao"])
    ].arquivo.to_list()
    data = datetime.strptime(lista[-1], "%Y%m")
    return data.strftime("%Y-%m-01")


@task
def check_condition(con1: bool, con2: bool):
    return con1 is True or con2 is True


@task
def check_for_updates(df):
    """
    Checks for outdated tables.
    """
    return df.query("desatualizado == True").arquivo.to_list()


@task
def files_to_download(df):
    log("Arquivos na fila para o download -->")
    log(
        df[
            df["ultima_atualizacao"] == max(df["ultima_atualizacao"])
        ].arquivo.to_list()
    )
    return df[
        df["ultima_atualizacao"] == max(df["ultima_atualizacao"])
    ].arquivo.to_list()


@task
def crawler_ans(files):
    for file in tqdm(files):
        urls, zips = get_url_from_template(file)

        save_path = "/tmp/data/br_ans_beneficiario/beneficiario/input/"
        os.makedirs(save_path, exist_ok=True)
        log(f"`mkdir = True` >>> {save_path} directory was created.")
        asyncio.run(download_files_async(urls, save_path, "zip"))

        log(f"DOWNLOADED FILE ->>> {file}")

        parquet_partition(
            path="/tmp/data/br_ans_beneficiario/beneficiario/input/"
        )

        os.system(
            'cd /tmp/data/br_ans_beneficiario/beneficiario/input; find . -type f -iname "*.csv" -delete'
        )
        log(f"CSV`s files have been deleted for ->>> {file}")

    return "/tmp/data/br_ans_beneficiario/output/"


@task
def is_empty(lista):
    if len(lista) == 0:
        return True
    else:
        return False


@task
def get_output():
    """
    Returns the output path.
    """
    return "/tmp/data/br_ans_beneficiario/output/"
