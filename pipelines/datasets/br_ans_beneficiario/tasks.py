# -*- coding: utf-8 -*-
"""
Tasks for br_ans_beneficiario
"""
import pandas as pd
from multiprocessing import Pool
from datetime import datetime
from loguru import logger
from pathlib import Path
from ftputil import FTPHost
from functools import reduce
from prefect import task
import os
from tqdm import tqdm
import zipfile
import requests
from glob import glob
from pipelines.datasets.br_ans_beneficiario.utils import (
    host_months_path,
    host_list,
    host,
    read_csv_zip_to_dataframe,
    process,
    RAW_COLLUNS_TYPE,
)
from pipelines.utils.utils import (
    log,
    to_partitions,
)


@task
def get_url_from_template(year: int, month: int) -> str:
    """Return the url for the PNAD microdata file for a given year and month.
    Args:
        year (int): Year of the microdata file.
        quarter (int): Quarter of the microdata file.
    Returns:
        str: url
    """
    download_page = f"https://dadosabertos.ans.gov.br/FTP/PDA/informacoes_consolidadas_de_beneficiarios/{year}0{month}/"
    response = requests.get(download_page, timeout=5)

    if response.status_code >= 400 and response.status_code <= 599:
        raise Exception(f"Erro de requisição: status code {response.status_code}")

    else:
        hrefs = [k for k in response.text.split('href="')[1:] if "zip" in k]
        hrefs = [k.split('"')[0] for k in hrefs]
    # filename = None
    # for href in hrefs:
    #    if f"{year}0{month}" in href:
    #         filename = href
    # if not filename:
    #    raise Exception("Erro: o atributo href não existe.")

    # url = "https://ftp.ibge.gov.br/Trabalho_e_Rendimento/Pesquisa_Nacional_por_Amostra_de_Domicilios_continua/Trimestral/Microdados" + "/{year}/{filename}"
    return hrefs


@task
def download_unzip_csv(
    url: str, files, chunk_size: int = 128, mkdir: bool = True, id="teste"
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

    if isinstance(files, list):
        for file in files:
            logger.info(f"Baixando o arquivo {file}")
            download_url = f"{url}{file}"
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
                logger.info("Dados extraídos com sucesso!")

            except zipfile.BadZipFile:
                logger.info(f"O arquivo {file} não é um arquivo ZIP válido.")

            os.system(
                f'cd /tmp/data/br_ans_beneficiario/{id}/input; find . -type f ! -iname "*.csv" -delete'
            )

    elif isinstance(files, str):
        logger.info(f"Baixando o arquivo {files}")
        download_url = f"{url}{files}"
        save_path = f"/tmp/data/br_ans_beneficiario/{id}/input/{files}"

        r = requests.get(download_url, headers=request_headers, stream=True, timeout=10)
        with open(save_path, "wb") as fd:
            for chunk in tqdm(r.iter_content(chunk_size=chunk_size)):
                fd.write(chunk)

        try:
            with zipfile.ZipFile(save_path) as z:
                z.extractall(f"/tmp/data/br_ans_beneficiario/{id}/input")
            logger.info("Dados extraídos com sucesso!")

        except zipfile.BadZipFile:
            (f"O arquivo {files} não é um arquivo ZIP válido.")

        os.system(
            f'cd /tmp/data/br_ans_beneficiario/{id}/input; find . -type f ! -iname "*.csv" -delete'
        )

    else:
        raise ValueError("O argumento 'files' possui um tipo inadequado.")

    return f"/tmp/data/br_ans_beneficiario/{id}/input/"


@task
def clean_ans(path):
    for nome_arquivo in os.listdir(path):
        if nome_arquivo.endswith(".csv"):
            logger.info(f"Carregando o arquivo: {nome_arquivo}")
            df = pd.read_csv(
                f"{path}{nome_arquivo}",
                sep=";",
                encoding="cp1252",
                dtype=RAW_COLLUNS_TYPE,
            )
            df = process(df)
            logger.info("Cleaning dataset")
            os.makedirs("/tmp/data/br_ans_beneficiario/output/", exist_ok=True)
            to_partitions(
                df,
                partition_columns=["ano", "mes", "sigla_uf"],
                savepath="/tmp/data/br_ans_beneficiario/output/",
            )
            logger.info("Partição feita.")


@task  # noqa
def ans(data_inicio: str, data_fim: str):
    logger.info(f"Data início: {data_inicio} ---->  Data fim: {data_fim}.")
    for month_path, month_date in host_months_path(data_inicio, data_fim):
        # print(month_path, month_date)
        for state_path in host_list(month_path):
            state = state_path.split("_")[-1].split(".")[0]
            # print(state, month_path)
            output_path = (
                Path("/tmp/data/output")
                / f'ano={month_date.strftime("%Y")}'
                / f'mes={month_date.strftime("%m")}'
                / f"sigla_uf={state}"
                / f'ben{month_date.strftime("%Y%m")}-{state}.csv'
            )

            if output_path.exists():
                logger.info(f"Jumping path {output_path}. Already download")
                continue
            input_path = (
                Path("/tmp/data/input")
                / f'ano={month_date.strftime("%Y")}'
                / f'mes={month_date.strftime("%m")}'
                / f'ben{month_date.strftime("%Y%m")}_{state}.csv'
            )
            # print(input_path)
            if input_path.exists():
                logger.debug(f"reading input in {input_path}")
                df = pd.read_csv(
                    input_path, encoding="utf-8", dtype=RAW_COLLUNS_TYPE, index_col=0
                )
            else:
                # Wtf, pq tem um repositorio aqui?
                # https://dadosabertos.ans.gov.br/FTP/PDA/informacoes_consolidadas_de_beneficiarios/201602/201607/
                if not host.path.isfile(state_path):
                    continue
                df = read_csv_zip_to_dataframe(state_path)
                # input_path.parent.mkdir(parents=True, exist_ok=True)
                # df.to_csv(input_path, encoding="utf-8")

                logger.info("Cleaning dataset")
                df = process(df)
                output_path.parent.mkdir(parents=True, exist_ok=True)

                # delete partition columns
                del df["ano"]
                del df["mes"]
                del df["sigla_uf"]

                logger.info(f"Writing to output {output_path.as_posix()}")
                df.to_csv(output_path, index=False)

    return "/tmp/data/output/"
