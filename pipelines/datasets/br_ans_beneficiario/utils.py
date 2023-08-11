# -*- coding: utf-8 -*-
"""
General purpose functions for the br_ans_beneficiario project
"""
import pandas as pd

# from multiprocessing import Pool
from loguru import logger
from pathlib import Path


from datetime import datetime
from io import BytesIO
import os
from tqdm import tqdm

# import tempfile
from pipelines.datasets.br_ans_beneficiario.constants import constants as ans_constants
import zipfile
import requests
from functools import reduce
from dateutil.relativedelta import relativedelta
from pipelines.utils.utils import (
    log,
    to_partitions,
)
import unidecode


def remove_accents(a):
    return unidecode.unidecode(a).lower()


def process(df: pd.DataFrame):
    time_col = pd.to_datetime(df["#ID_CMPT_MOVEL"], format="%Y%m")
    df["ano"] = time_col.dt.year
    df["mes"] = time_col.dt.month
    del df["#ID_CMPT_MOVEL"]
    del df["NM_MUNICIPIO"]
    del df["DT_CARGA"]

    df.rename(
        columns={
            "CD_OPERADORA": "codigo_operadora",
            "NM_RAZAO_SOCIAL": "razao_social",
            "NR_CNPJ": "cnpj",
            "MODALIDADE_OPERADORA": "modalidade_operadora",
            "SG_UF": "sigla_uf",
            "CD_MUNICIPIO": "id_municipio_6",
            "TP_SEXO": "sexo",
            "DE_FAIXA_ETARIA": "faixa_etaria",
            "DE_FAIXA_ETARIA_REAJ": "faixa_etaria_reajuste",
            "CD_PLANO": "codigo_plano",
            "TP_VIGENCIA_PLANO": "tipo_vigencia_plano",
            "DE_CONTRATACAO_PLANO": "contratacao_beneficiario",
            "DE_SEGMENTACAO_PLANO": "segmentacao_beneficiario",
            "DE_ABRG_GEOGRAFICA_PLANO": "abrangencia_beneficiario",
            "COBERTURA_ASSIST_PLAN": "cobertura_assistencia_beneficiario",
            "TIPO_VINCULO": "tipo_vinculo",
            "QT_BENEFICIARIO_ATIVO": "quantidade_beneficiario_ativo",
            "QT_BENEFICIARIO_ADERIDO": "quantidade_beneficiario_aderido",
            "QT_BENEFICIARIO_CANCELADO": "quantidade_beneficiario_cancelado",
        },
        inplace=True,
    )

    df["cnpj"] = df["cnpj"].str.zfill(14)

    # Using parquet, don't need external dictionary
    df["tipo_vigencia_plano"].replace(
        {
            "P": "Posterior à Lei 9656/1998 ou planos adaptados à lei",
            "A": "Anterior à Lei 9656/1998",
        }
    )

    return df


def get_url_from_template(file) -> str:
    """
    Return the URL for the ANS microdata file for a given year and quarter.

    Args:
        file (str): Name of the microdata file, which should be in the format "YYYYMM", representing the year and month.

    Returns:
        str: URL of the microdata file.

    Raises:
        Exception: If there is an error in the HTTP request with status code 4xx or 5xx.
    """
    download_page = f"https://dadosabertos.ans.gov.br/FTP/PDA/informacoes_consolidadas_de_beneficiarios/{file}/"
    response = requests.get(download_page, timeout=5)

    if response.status_code >= 400 and response.status_code <= 599:
        raise Exception(f"Erro de requisição: status code {response.status_code}")

    else:
        hrefs = [k for k in response.text.split('href="')[1:] if "zip" in k]
        zips = [k.split('"')[0] for k in hrefs]
        hrefs = [f"{download_page}{k}" for k in zips]
    return [hrefs, zips]


def download_unzip_csv(
    urls, zips, chunk_size: int = 128, mkdir: bool = True, id="teste"
) -> str:
    """
    Downloads and unzips a .csv file from a given list of files and saves it to a local directory.
    Parameters:
    -----------
    url: str
        The base URL from which to download the files.
    files: list or str
        The .zip file names or a single .zip file name to download the csv file from.
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
        os.makedirs(f"/tmp/data/br_ans_beneficiario/{id}/input/", exist_ok=True)

    request_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36",
    }

    if isinstance(urls, list):
        for url, file in zip(urls, zips):
            log(f"Baixando o arquivo {file}")
            download_url = url
            save_path = f"/tmp/data/br_ans_beneficiario/{id}/input/{file}"

            r = requests.get(
                download_url, headers=request_headers, stream=True, timeout=50
            )
            with open(save_path, "wb") as fd:
                for chunk in tqdm(r.iter_content(chunk_size=chunk_size)):
                    fd.write(chunk)

            try:
                with zipfile.ZipFile(save_path) as z:
                    z.extractall(f"/tmp/data/br_ans_beneficiario/{id}/input")
                log("Dados extraídos com sucesso!")

            except zipfile.BadZipFile:
                log(f"O arquivo {file} não é um arquivo ZIP válido.")

            os.system(
                f'cd /tmp/data/br_ans_beneficiario/{id}/input; find . -type f ! -iname "*.csv" -delete'
            )

    elif isinstance(urls, str):
        log(f"Baixando o arquivo {urls}")
        download_url = urls
        save_path = f"/tmp/data/br_ans_beneficiario/{id}/input/{zips}"

        r = requests.get(download_url, headers=request_headers, stream=True, timeout=10)
        with open(save_path, "wb") as fd:
            for chunk in tqdm(r.iter_content(chunk_size=chunk_size)):
                fd.write(chunk)

        try:
            with zipfile.ZipFile(save_path) as z:
                z.extractall(f"/tmp/data/br_ans_beneficiario/{id}/input")
            log("Dados extraídos com sucesso!")

        except zipfile.BadZipFile:
            (f"O arquivo {zips} não é um arquivo ZIP válido.")

        os.system(
            f'cd /tmp/data/br_ans_beneficiario/{id}/input; find . -type f ! -iname "*.csv" -delete'
        )

    else:
        raise ValueError("O argumento 'files' possui um tipo inadequado.")


def parquet_partition(path):
    # dfs = []
    for nome_arquivo in os.listdir(path):
        if nome_arquivo.endswith(".csv"):
            log(f"Carregando o arquivo: {nome_arquivo}")
            df = pd.read_csv(
                f"{path}{nome_arquivo}",
                sep=";",
                encoding="cp1252",
                dtype=ans_constants.RAW_COLLUNS_TYPE.value,
            )
            # df = process(df)
            time_col = pd.to_datetime(df["#ID_CMPT_MOVEL"], format="%Y%m")
            df["ano"] = time_col.dt.year
            df["mes"] = time_col.dt.month
            df["MODALIDADE_OPERADORA"] = df["MODALIDADE_OPERADORA"].apply(
                remove_accents
            )
            df.rename(
                columns={
                    "SG_UF": "sigla_uf",
                    "MODALIDADE_OPERADORA": "modalidade_operadora",
                },
                inplace=True,
            )

            log("Lendo dataset")
            os.makedirs("/tmp/data/br_ans_beneficiario/output/", exist_ok=True)

            to_partitions(
                df,
                partition_columns=["ano", "mes", "sigla_uf", "modalidade_operadora"],
                savepath="/tmp/data/br_ans_beneficiario/output/",
                file_type="parquet",
            )

            log("Partição feita.")

    return "/tmp/data/br_ans_beneficiario/output/"
