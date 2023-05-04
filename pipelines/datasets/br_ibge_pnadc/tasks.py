# -*- coding: utf-8 -*-
"""
Tasks for br_ibge_pnadc
"""
# pylint: disable=invalid-name,unnecessary-dunder-call
import zipfile
import os
from glob import glob

import requests
from tqdm import tqdm
import pandas as pd
import numpy as np
from prefect import task

from pipelines.utils.utils import log
from pipelines.datasets.br_ibge_pnadc.constants import constants as pnad_constants


@task
def get_url_from_template(year: int, quarter: int) -> str:
    """Return the url for the PNAD microdata file for a given year and month.
    Args:
        year (int): Year of the microdata file.
        quarter (int): Quarter of the microdata file.
    Returns:
        str: url
    """
    download_page = f"https://ftp.ibge.gov.br/Trabalho_e_Rendimento/Pesquisa_Nacional_por_Amostra_de_Domicilios_continua/Trimestral/Microdados/{year}/"
    response = requests.get(download_page, timeout=5)

    if response.status_code >= 400 and response.status_code <= 599:
        raise Exception(f"Erro de requisição: status code {response.status_code}")

    else:
        hrefs = [k for k in response.text.split('href="')[1:] if "zip" in k]
        hrefs = [k.split('"')[0] for k in hrefs]
        filename = None
        for href in hrefs:
            if f"0{quarter}{year}" in href:
                filename = href
        if not filename:
            raise Exception("Erro: o atributo href não existe.")

        url = pnad_constants.URL_PREFIX.value + "/{year}/{filename}"
    return url.format(year=year, filename=filename)


@task
def download_txt(url, chunk_size=128, mkdir=False) -> str:
    """
    Gets all csv files from a url and saves them to a directory.
    """
    if mkdir:
        os.system("mkdir -p /tmp/data/input/")

    request_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36",
    }
    r = requests.get(url, headers=request_headers, stream=True, timeout=10)
    save_path = "/tmp/data/"
    save_path = save_path + url.split("/")[-1]
    with open(save_path, "wb") as fd:
        for chunk in tqdm(r.iter_content(chunk_size=chunk_size)):
            fd.write(chunk)

    with zipfile.ZipFile(save_path) as z:
        z.extractall("/tmp/data/input")
    os.system('cd /tmp/data/input; find . -type f ! -iname "*.txt" -delete')
    filepath = glob("/tmp/data/input/*.txt")[0]

    log(f"Using file {filepath}")

    return filepath


@task
def build_parquet_files(filepath: str) -> str:
    """
    Build parquets from txt original file.
    """

    os.system("mkdir -p /tmp/data/staging/")
    # read file
    chunks = pd.read_fwf(
        filepath,
        widths=pnad_constants.COLUMNS_WIDTHS.value,
        names=pnad_constants.COLUMNS_NAMES.value,
        header=None,
        encoding="utf-8",
        dtype=str,
        chunksize=10000,
    )

    for i, chunk in enumerate(chunks):
        # partition by year, quarter and region
        chunk.rename(
            columns={
                "UF": "id_uf",
                "Estrato": "id_estrato",
                "UPA": "id_upa",
                "Capital": "capital",
                "RM_RIDE": "rm_ride",
                "Trimestre": "trimestre",
                "Ano": "ano",
            },
            inplace=True,
        )
        chunk["sigla_uf"] = chunk["id_uf"].map(pnad_constants.map_codigo_sigla_uf.value)
        chunk["id_domicilio"] = chunk["id_estrato"] + chunk["V1008"] + chunk["V1014"]

        chunk["habitual"] = [np.nan] * len(chunk)
        chunk["efetivo"] = [np.nan] * len(chunk)
        ordered_columns = pnad_constants.COLUMNS_ORDER.value
        chunk = chunk[ordered_columns]

        # save to parquet
        chunk.to_parquet(
            f"/tmp/data/staging/microdados_{i}.parquet",
            index=False,
        )

    # print number of parquet files
    total_files = len(glob("/tmp/data/staging/*.parquet"))
    log(f"Total of {total_files} parquet files created.")

    return "/tmp/data/staging/"


@task
def save_partitions(filepath: str) -> str:
    """
    Save partitions to disk.

    Args:
        filepath (str): Path to the file used to build the partitions.

    Returns:
        str: Path to the saved file.

    """
    os.system("mkdir -p /tmp/data/output/")

    # get all parquet files
    parquet_files = glob(f"{filepath}*.parquet")
    # read all parquet files
    df = pd.concat([pd.read_parquet(f) for f in parquet_files])

    trimestre = df["trimestre"].unique()[0]
    ano = df["ano"].unique()[0]
    df.drop(columns=["trimestre", "ano"], inplace=True)
    ufs = df["sigla_uf"].unique()

    for uf in ufs:
        df_uf = df[df["sigla_uf"] == uf]
        df_uf.drop(columns=["sigla_uf"], inplace=True)
        os.system(
            "mkdir -p /tmp/data/output/ano={ano}/trimestre={trimestre}/sigla_uf={uf}".format(
                ano=ano, trimestre=trimestre, uf=uf
            )
        )
        df_uf.to_csv(
            f"/tmp/data/output/ano={ano}/trimestre={trimestre}/sigla_uf={uf}/microdados.csv",
            index=False,
        )

    return "/tmp/data/output/"
