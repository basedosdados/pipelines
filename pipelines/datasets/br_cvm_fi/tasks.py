# -*- coding: utf-8 -*-
"""
Tasks for br_cvm_fi
"""

from prefect import task
import pandas as pd
import os
from datetime import datetime
import requests
from tqdm import tqdm
import zipfile
from bs4 import BeautifulSoup
import re
import glob
from pipelines.datasets.br_cvm_fi.utils import (
    sheet_to_df,
    rename_columns,
    check_and_create_column,
    limpar_string,
    obter_anos_meses,
)
from pipelines.utils.utils import (
    log,
    to_partitions,
)
from pipelines.datasets.br_cvm_fi.constants import constants as cvm_constants


@task  # noqa
def download_unzip_csv(
    url: str, files, chunk_size: int = 128, mkdir: bool = True
) -> str:
    """
    Downloads and unzip a .csv file from a given list of files and saves it to a local directory.
    Parameters:
    -----------
    files: list
        The .zip file names to download the csv file from.
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
        os.system("mkdir -p /tmp/data/br_cvm_fi/input/")

    request_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36",
    }
    if type(files) == list:
        for file in files:
            log(f"Baixando para o arquivo {file}")
            url = f"{url}{file}"
            r = requests.get(url, headers=request_headers, stream=True, timeout=10)
            save_path = "/tmp/data/br_cvm_fi/input/file.zip"
            with open(save_path, "wb") as fd:
                for chunk in tqdm(r.iter_content(chunk_size=chunk_size)):
                    fd.write(chunk)

            with zipfile.ZipFile(save_path) as z:
                z.extractall("/tmp/data/br_cvm_fi/input")
            os.system(
                'cd /tmp/data/br_cvm_fi/input; find . -type f ! -iname "*.csv" -delete'
            )

            log("Dados baixados com sucesso!")
    elif type(files) == str:
        log(f"Baixando para o arquivo {files}")
        url = f"{url}{files}"
        r = requests.get(url, headers=request_headers, stream=True, timeout=10)
        save_path = "/tmp/data/br_cvm_fi/input/file.zip"
        with open(save_path, "wb") as fd:
            for chunk in tqdm(r.iter_content(chunk_size=chunk_size)):
                fd.write(chunk)

        with zipfile.ZipFile(save_path) as z:
            z.extractall("/tmp/data/br_cvm_fi/input")
        os.system(
            'cd /tmp/data/br_cvm_fi/input; find . -type f ! -iname "*.csv" -delete'
        )

        log("Dados baixados com sucesso!")
    else:
        raise ValueError("Argument `files` have nappropriate type")

    return "/tmp/data/br_cvm_fi/input/"


@task
def extract_links_and_dates(url) -> pd.DataFrame:
    """
    Extracts all file names and their respective last update dates in a pandas dataframe.
    """

    response = requests.get(url)
    soup = BeautifulSoup(response.content, "html.parser")

    # Encontra todos os links dentro do HTML
    links = soup.find_all("a")
    links_zip = []

    for link in links:
        if link.has_attr("href") and link["href"].endswith(".zip"):
            links_zip.append(link["href"])

    # Encontra todas as datas de atualização dentro do HTML
    padrao = r"\d{2}-\w{3}-\d{4} \d{2}:\d{2}"
    datas = soup.find_all(string=lambda text: re.findall(padrao, text))
    datas_atualizacao = []

    for data in datas:
        data_atualizacao = re.findall(padrao, data)[0]
        datas_atualizacao.append(data_atualizacao)

    dados = {
        "arquivo": links_zip,
        "ultima_atualizacao": datas_atualizacao[1:],
        "data_hoje": datetime.now().strftime("%Y-%m-%d"),
    }
    df = pd.DataFrame(dados)
    df.ultima_atualizacao = df.ultima_atualizacao.apply(
        lambda x: datetime.strptime(x, "%d-%b-%Y %H:%M").strftime("%Y-%m-%d")
    )

    df["desatualizado"] = df["data_hoje"] == df["ultima_atualizacao"]

    # df['desatualizado'] = df['arquivo'].apply(lambda x: True if x in ['inf_diario_fi_202201.zip','inf_diario_fi_202305.zip'] else False)
    return df


@task
def check_for_updates(df):
    """
    Checks for outdated tables.
    """

    return df.query("desatualizado == True").arquivo.to_list()


@task
def is_empty(lista):
    if len(lista) == 0:
        return True
    else:
        return False


@task
def clean_data_and_make_partitions(path: str) -> str:
    """
    Clean cvm data based on architecture file and make partitions.
    """

    os.chdir(path)
    files = glob.glob("*.csv")
    df_arq = sheet_to_df(
        "https://docs.google.com/spreadsheets/d/1W739_mLZNBPYhBqGyjsuuWFBOqCrhrDl/edit#gid=1045172528"
    )  # constant

    for file in files:
        df = pd.read_csv(f"{path}{file}", sep=";")
        log(f"File {file} read.")
        df = rename_columns(df_arq, df)
        df["ano"] = df["data_competencia"].apply(
            lambda x: datetime.strptime(x, "%Y-%m-%d").year
        )
        df["mes"] = df["data_competencia"].apply(
            lambda x: datetime.strptime(x, "%Y-%m-%d").month
        )
        log(f"File {file} cleaned.")
        to_partitions(
            df,
            partition_columns=["ano", "mes"],
            savepath="/tmp/data/br_cvm_fi/output/",
        )  # constant
        log("Partition created.")

    return "/tmp/data/br_cvm_fi/output/"


@task
def clean_data_make_partitions_cda(diretorio):
    df_arq = sheet_to_df(cvm_constants.ARQUITETURA_URL.value)
    anos_meses = obter_anos_meses(diretorio)

    for i in anos_meses:
        df_final = pd.DataFrame()
        arquivos = glob.glob(f"{diretorio}*.csv")
        padrao = diretorio + f"cda_fi_BLC_[1-8]_{i}.csv"
        arquivos_filtrados = [arq for arq in arquivos if re.match(padrao, arq)]

        for file in tqdm(arquivos_filtrados):
            print(f"Baixando o arquivo ------> {file}")

            df = pd.read_csv(file, sep=";", encoding="ISO-8859-1", dtype="string")
            df["ano"] = df["DT_COMPTC"].apply(
                lambda x: datetime.strptime(x, "%Y-%m-%d").year
            )
            df["mes"] = df["DT_COMPTC"].apply(
                lambda x: datetime.strptime(x, "%Y-%m-%d").month
            )

            # pattern = r"(BLC_[1-8])"

            match = re.search(r"(BLC_[1-8])", file)
            df["bloco"] = match.group(1)

            df_final = pd.concat([df_final, df], ignore_index=True)

        df_final = check_and_create_column(
            df_final, colunas_totais=cvm_constants.COLUNAS_FINAL.value
        )
        df_final[cvm_constants.COLUNAS.value] = df_final[
            cvm_constants.COLUNAS.value
        ].applymap(lambda x: cvm_constants.MAPEAMENTO.value.get(x, x))
        df_final["CNPJ_FUNDO"] = df_final["CNPJ_FUNDO"].str.replace(r"[/.-]", "")
        df_final["CNPJ_INSTITUICAO_FINANC_COOBR"] = df_final[
            "CNPJ_INSTITUICAO_FINANC_COOBR"
        ].str.replace(r"[/.-]", "")
        df_final["CPF_CNPJ_EMISSOR"] = df_final["CPF_CNPJ_EMISSOR"].str.replace(
            r"[/.-]", ""
        )
        df_final["CNPJ_EMISSOR"] = df_final["CNPJ_EMISSOR"].str.replace(r"[/.-]", "")
        df_final["CNPJ_FUNDO_COTA"] = df_final["CNPJ_FUNDO_COTA"].str.replace(
            r"[/.-]", ""
        )
        df_final = rename_columns(df_arq, df_final)
        df_final = df_final.replace(",", ".", regex=True)
        df_final[cvm_constants.COLUNAS_ASCI.value] = df_final[
            cvm_constants.COLUNAS_ASCI.value
        ].fillna("")
        df_final[cvm_constants.COLUNAS_ASCI.value] = df_final[
            cvm_constants.COLUNAS_ASCI.value
        ].applymap(limpar_string)
        df_final = df_final[cvm_constants.COLUNAS_TOTAIS.value]
        print(f"Fazendo partições para o ano ------> {i}")
        to_partitions(
            df_final,
            partition_columns=["ano", "mes"],
            savepath="/tmp/data/br_cvm_fi/output/",
        )  # constant
