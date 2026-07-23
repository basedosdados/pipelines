"""
General purpose functions for the br_ans_beneficiario project
"""

import gc
import os
import zipfile

import pandas as pd
import requests
import unidecode
from tqdm import tqdm

from pipelines.crawler.ans_beneficiario.constants import (
    constants as ans_constants,
)
from pipelines.utils.utils import log, to_partitions


def remove_accents(text):
    return unidecode.unidecode(text).lower()


def get_url_from_template(file) -> str:
    """
    Return the URL for the ANS microdata file for a given year and quarter.
    """
    download_page = f"https://dadosabertos.ans.gov.br/FTP/PDA/informacoes_consolidadas_de_beneficiarios-024/{file}/"
    response = requests.get(download_page, timeout=5)

    if response.status_code >= 400 and response.status_code <= 599:
        raise Exception(
            f"Erro de requisição: status code {response.status_code}"
        )

    hrefs = [k for k in response.text.split('href="')[1:] if "zip" in k]
    zips = [k.split('"')[0] for k in hrefs]
    hrefs = [f"{download_page}{k}" for k in zips]
    # pyrefly: ignore [bad-return]
    return [hrefs, zips]


def download_unzip_csv(
    urls,
    zips,
    chunk_size: int = 128,
    mkdir: bool = True,
    id="teste",
    # pyrefly: ignore [bad-return]
) -> str:
    if mkdir:
        os.makedirs(
            f"/tmp/data/br_ans_beneficiario/{id}/input/", exist_ok=True
        )

    request_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36",
    }

    if isinstance(urls, list):
        for url, file in zip(urls, zips, strict=False):
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

            # pyrefly: ignore [deprecated]
            os.system(
                f'cd /tmp/data/br_ans_beneficiario/{id}/input; find . -type f ! -iname "*.csv" -delete'
            )

            gc.collect()

    elif isinstance(urls, str):
        log(f"Baixando o arquivo {urls}")
        download_url = urls
        save_path = f"/tmp/data/br_ans_beneficiario/{id}/input/{zips}"

        r = requests.get(
            download_url, headers=request_headers, stream=True, timeout=10
        )
        with open(save_path, "wb") as fd:
            for chunk in tqdm(r.iter_content(chunk_size=chunk_size)):
                fd.write(chunk)

        try:
            with zipfile.ZipFile(save_path) as z:
                z.extractall(f"/tmp/data/br_ans_beneficiario/{id}/input")
            log("Dados extraídos com sucesso!")

        except zipfile.BadZipFile:
            log(f"O arquivo {zips} não é um arquivo ZIP válido.")

        # pyrefly: ignore [deprecated]
        os.system(
            f'cd /tmp/data/br_ans_beneficiario/{id}/input; find . -type f ! -iname "*.csv" -delete'
        )

    else:
        raise ValueError("O argumento 'files' possui um tipo inadequado.")


def parquet_partition(path):
    for nome_arquivo in os.listdir(path):
        if nome_arquivo.endswith(".csv"):
            log(f"Carregando o arquivo: {nome_arquivo}")

            # pyrefly: ignore [no-matching-overload]
            df = pd.read_csv(
                f"{path}{nome_arquivo}",
                sep=";",
                encoding="utf-8",
                dtype=ans_constants.RAW_COLLUNS_TYPE.value,
            )
            try:
                time_col = pd.to_datetime(df["ID_CMPT_MOVEL"], format="%Y%m")
            except:  # noqa: E722
                log(
                    "parsing ID_CMPT_MOVEL data failed with pattern %Y%m. Trying %Y-%m"
                )
                time_col = pd.to_datetime(df["ID_CMPT_MOVEL"], format="%Y-%m")

            df["ano"] = time_col.dt.year
            df["mes"] = time_col.dt.month
            df["MODALIDADE_OPERADORA"] = df["MODALIDADE_OPERADORA"].apply(
                remove_accents
            )
            df = df.rename(
                columns={
                    "SG_UF": "sigla_uf",
                    "MODALIDADE_OPERADORA": "modalidade_operadora",
                }
            )

            log("Lendo dataset")
            os.makedirs("/tmp/data/br_ans_beneficiario/output/", exist_ok=True)

            to_partitions(
                df,
                partition_columns=[
                    "ano",
                    "mes",
                    "sigla_uf",
                    "modalidade_operadora",
                ],
                savepath="/tmp/data/br_ans_beneficiario/output/",
                file_type="parquet",
            )

            log("Partição feita.")

    return "/tmp/data/br_ans_beneficiario/output/"
