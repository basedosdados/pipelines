"""
Tasks for br_ans_beneficiario — Prefect 3.
"""

import asyncio
import gc
import os
import re
from datetime import datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup
from prefect import task
from tqdm import tqdm

from pipelines.crawler.ans_beneficiario.utils import (
    get_url_from_template,
    parquet_partition,
)
from pipelines.utils.to_download.utils import download_files_async
from pipelines.utils.utils import log


@task
def extract_links_and_dates(url) -> pd.DataFrame:
    """
    Extracts all file names and their respective last update dates.
    """
    response = requests.get(url)

    if response.status_code != 200:
        # pyrefly: ignore [missing-argument]
        raise requests.HTTPError(
            f"Erro HTTP: A resposta da API malsucedida. O código retornado foi:  {response.status_code}"
        )

    soup = BeautifulSoup(response.content, "html.parser")

    links = soup.find_all("a")
    links_zip = []
    for link in links:
        if link.has_attr("href") and link["href"].startswith("20"):
            links_zip.append(link["href"].replace("/", ""))

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
    # pyrefly: ignore [missing-attribute]
    df.ultima_atualizacao = df.ultima_atualizacao.apply(
        lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M").strftime("%Y-%m-%d")
    )

    df["desatualizado"] = df["data_hoje"] == df["ultima_atualizacao"]

    return df


@task
def get_file_max_date(df):
    lista = df[
        df["ultima_atualizacao"] == max(df["ultima_atualizacao"])
    ].arquivo.to_list()
    data = datetime.strptime(lista[-1], "%Y%m")
    return data.strftime("%Y-%m")


@task
def files_to_download(df, year):
    if year is not None:
        log("Arquivos na fila para o download -->")
        df = df[df["arquivo"].str.contains(year)]
        log(df["arquivo"].unique().tolist())

        return df["arquivo"].unique().tolist()

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
        urls, _ = get_url_from_template(file)

        save_path = "/tmp/data/br_ans_beneficiario/beneficiario/input/"
        os.makedirs(save_path, exist_ok=True)
        log(f"`mkdir = True` >>> {save_path} directory was created.")
        # pyrefly: ignore [bad-argument-type]
        asyncio.run(download_files_async(urls, save_path, "zip"))

        log(f"DOWNLOADED FILE ->>> {file}")

        os.makedirs(
            "/tmp/data/br_ans_beneficiario/beneficiario/input/", exist_ok=True
        )

        parquet_partition(
            path="/tmp/data/br_ans_beneficiario/beneficiario/input/"
        )

        # pyrefly: ignore [deprecated]
        os.system(
            'cd /tmp/data/br_ans_beneficiario/beneficiario/input; find . -type f -iname "*.csv" -delete'
        )
        log(f"CSV`s files have been deleted for ->>> {file}")

        gc.collect()

    return "/tmp/data/br_ans_beneficiario/output/"
