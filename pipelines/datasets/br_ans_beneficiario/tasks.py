# -*- coding: utf-8 -*-
"""
Tasks for br_ans_beneficiario
"""
import os
import re
from datetime import datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup
from loguru import logger
import asyncio
from prefect import task
from pipelines.utils.to_download.utils import download_files_async
from tqdm import tqdm
from pipelines.utils.to_download.tasks import to_download

from pipelines.datasets.br_ans_beneficiario.constants import constants as ans_constants
from pipelines.datasets.br_ans_beneficiario.utils import (
    download_unzip_csv,
    get_url_from_template,
    parquet_partition,
)
from pipelines.utils.utils import log, to_partitions


@task
def extract_links_and_dates(url) -> pd.DataFrame:
    """
    Extracts all file names and their respective last update dates in a pandas dataframe.
    """

    response = requests.get(url)
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
def return_last_date(df):
    return max(df['ultima_atualizacao'])


@task
def check_for_updates(df):
    """
    Checks for outdated tables.
    """
    log(f"Data de hoje --> {max(df['data_hoje'])}")
    log(f"Data de atualização --> {max(df['ultima_atualizacao'])}")
    return df.query("desatualizado == True").arquivo.to_list()


@task
def force_update(df):
    log(df[df['ultima_atualizacao'] == max(df['ultima_atualizacao'])].arquivo.to_list())
    return df[df['ultima_atualizacao'] == max(df['ultima_atualizacao'])].arquivo.to_list()

@task
def crawler_ans(files):
    for file in tqdm(files):
        urls, zips = get_url_from_template(file)

        save_path = "/tmp/data/br_ans_beneficiario/beneficiario/input/"
        os.makedirs(save_path, exist_ok=True)
        log(f"`mkdir = True` >>> {save_path} directory was created.")
        asyncio.run(
            download_files_async(
                urls,
                save_path,
                "zip"
            )
        )

        log(f"DOWNLOADED FILE ->>> {file}")

        parquet_partition(path="/tmp/data/br_ans_beneficiario/beneficiario/input/")

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
