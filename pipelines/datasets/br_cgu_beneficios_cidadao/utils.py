# -*- coding: utf-8 -*-
import os
import zipfile

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import ElementNotInteractableException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from tqdm import tqdm

from pipelines.datasets.br_cgu_beneficios_cidadao.constants import constants
from pipelines.utils.utils import get_credentials_from_secret, log, to_partitions


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
        os.makedirs(f"/tmp/data/br_cgu_beneficios_cidadao/{id}/input/", exist_ok=True)

    request_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36",
    }

    if isinstance(files, list):
        for file in files:
            log(f"Baixando o arquivo {file}")
            download_url = f"{url}{file}"
            save_path = f"/tmp/data/br_cgu_beneficios_cidadao/{id}/input/{file}"

            r = requests.get(
                download_url, headers=request_headers, stream=True, timeout=50
            )
            with open(save_path, "wb") as fd:
                for chunk in tqdm(r.iter_content(chunk_size=chunk_size)):
                    fd.write(chunk)

            try:
                with zipfile.ZipFile(save_path) as z:
                    z.extractall(f"/tmp/data/br_cgu_beneficios_cidadao/{id}/input")
                log("Dados extraídos com sucesso!")

            except zipfile.BadZipFile:
                log(f"O arquivo {file} não é um arquivo ZIP válido.")

            os.system(
                f'cd /tmp/data/br_cgu_beneficios_cidadao/{id}/input; find . -type f ! -iname "*.csv" -delete'
            )

    elif isinstance(files, str):
        log(f"Baixando o arquivo {files}")
        download_url = f"{url}{files}"
        save_path = f"/tmp/data/br_cgu_beneficios_cidadao/{id}/input/{files}"

        r = requests.get(download_url, headers=request_headers, stream=True, timeout=10)
        with open(save_path, "wb") as fd:
            for chunk in tqdm(r.iter_content(chunk_size=chunk_size)):
                fd.write(chunk)

        try:
            with zipfile.ZipFile(save_path) as z:
                z.extractall(f"/tmp/data/br_cgu_beneficios_cidadao/{id}/input")
            log("Dados extraídos com sucesso!")

        except zipfile.BadZipFile:
            log(f"O arquivo {files} não é um arquivo ZIP válido.")

        os.system(
            f'cd /tmp/data/br_cgu_beneficios_cidadao/{id}/input; find . -type f ! -iname "*.csv" -delete'
        )

    else:
        raise ValueError("O argumento 'files' possui um tipo inadequado.")

    return f"/tmp/data/br_cgu_beneficios_cidadao/{id}/input/"


def extract_dates(table: str):
    driver = webdriver.Chrome()
    if table == "novo_bolsa_familia":
        driver.get(constants.ROOT_URL.value)
        driver.implicitly_wait(10)
    else:
        driver.get(constants.ROOT_URL_GARANTIA_SAFRA.value)
        driver.implicitly_wait(10)

    page_source = driver.page_source
    BeautifulSoup(page_source, "html.parser")

    select_anos = Select(driver.find_element(By.ID, "links-anos"))
    anos_meses = {}

    # Iterar sobre as opções dentro do select de anos
    for option_ano in select_anos.options:
        valor_ano = option_ano.get_attribute("value")
        ano = option_ano.text

        select_anos.select_by_value(valor_ano)

        driver.implicitly_wait(5)

        select_meses = Select(driver.find_element(By.ID, "links-meses"))

        meses_dict = {}

        # Iterar sobre as opções dentro do select de meses
        for option_mes in select_meses.options:
            valor_mes = option_mes.get_attribute("value")
            nome_mes = option_mes.text.capitalize()
            meses_dict[valor_mes] = nome_mes

        anos_meses[ano] = meses_dict

    driver.quit()

    anos = []
    meses_numeros = []
    meses_nomes = []

    # Iterar sobre o dicionário para extrair os dados
    for ano, meses in anos_meses.items():
        for mes_numero, mes_nome in meses.items():
            anos.append(ano)
            meses_numeros.append(mes_numero)
            meses_nomes.append(mes_nome)

    # Criar o DataFrame
    data = {"ano": anos, "mes_numero": meses_numeros, "mes_nome": meses_nomes}
    df = pd.DataFrame(data)
    df["urls"] = None
    for index, row in df.iterrows():
        if table == "novo_bolsa_familia":
            df["urls"][index] = f"{row.ano}{row.mes_numero}_NovoBolsaFamilia.zip"
        else:
            df["urls"][index] = f"{row.ano}{row.mes_numero}_GarantiaSafra.zip"
    return df


def parquet_partition(path: str, table: str):
    # dfs = []
    for nome_arquivo in os.listdir(path):
        if nome_arquivo.endswith(".csv"):
            log(f"Carregando o arquivo: {nome_arquivo}")
            # df = pd.read_csv(
            #    f"{path}{nome_arquivo}",
            #    sep=";",
            #    encoding="latin-1",
            #    dtype="string",
            # )

            df = None
            if table == "novo_bolsa_familia":
                with pd.read_csv(
                    f"{path}{nome_arquivo}",
                    sep=";",
                    encoding="latin-1",
                    dtype=constants.DTYPES_NOVO_BOLSA_FAMILIA.value,
                    chunksize=1000000,
                    decimal=",",
                ) as reader:
                    for chunk in tqdm(reader):
                        chunk.rename(
                            columns=constants.RENAMER_NOVO_BOLSA_FAMILIA.value,
                            inplace=True,
                        )
                        if df is None:
                            df = chunk
                        else:
                            df = pd.concat([df, chunk], axis=0)
            elif table == "garantia_safra":
                with pd.read_csv(
                    f"{path}{nome_arquivo}",
                    sep=";",
                    encoding="latin-1",
                    dtype=constants.DTYPES_GARANTIA_SAFRA.value,
                    chunksize=1000000,
                    decimal=",",
                ) as reader:
                    for chunk in tqdm(reader):
                        chunk.rename(
                            columns=constants.RENAMER_GARANTIA_SAFRA.value,
                            inplace=True,
                        )
                        if df is None:
                            df = chunk
                        else:
                            df = pd.concat([df, chunk], axis=0)

            # df.reset_index(drop=True, inplace=True)
            # df.columns = (df.columns.str.lower().str.replace(' ', '_').str.replace(r'[()\'\':_0-9]', ''))
            # time_col = pd.to_datetime(df["mês_competência"], format="%Y%m")
            # df["ano"] = time_col.dt.year
            # df["mes"] = time_col.dt.month
            # df.rename(columns={"uf":"sigla_uf"}, inplace = True)

            log("Lendo dataset")
            os.makedirs(
                f"/tmp/data/br_cgu_beneficios_cidadao/{table}/output/", exist_ok=True
            )
            if table == "novo_bolsa_familia":
                to_partitions(
                    df,
                    partition_columns=["mes_competencia", "sigla_uf"],
                    savepath=f"/tmp/data/br_cgu_beneficios_cidadao/{table}/output/",
                    file_type="parquet",
                )
            else:
                to_partitions(
                    df,
                    partition_columns=["mes_referencia"],
                    savepath=f"/tmp/data/br_cgu_beneficios_cidadao/{table}/output/",
                    file_type="parquet",
                )

            log("Partição feita.")

    return f"/tmp/data/br_cgu_beneficios_cidadao/{table}/output/"
