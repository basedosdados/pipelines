"""
Tasks for br_me_comex_stat
"""

import os
import re
import time as tm
from zipfile import ZipFile

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
from prefect import task

from pipelines.datasets.br_me_comex_stat.constants import (
    constants as comex_constants,
)
from pipelines.datasets.br_me_comex_stat.utils import (
    create_paths,
    download_data,
    validate_table,
)
from pipelines.utils.utils import log, to_partitions


@task
def parse_last_date(link: str) -> str:
    # Parsing do metadado que informa última atualização
    response = requests.get(link, verify=False)
    soup = BeautifulSoup(response.content, "html.parser")
    css_selector = "#parent-fieldname-text > h2"
    result = soup.select_one(css_selector)

    # Extrai mês e ano
    mes_ano = result.text.split("-")[-1].strip()

    # Extrai ano
    padrao = re.compile(r"\d+")
    numeros = padrao.findall(mes_ano)

    ano = numeros[0]

    #  Dicionário para converter data
    # informada na fonte oficial para mes ano no formato Y%-%m

    conversor = {
        f"janeiro de {ano}": f"{ano}-01",
        f"fevereiro de {ano}": f"{ano}-02",
        f"março de {ano}": f"{ano}-03",
        f"abril de {ano}": f"{ano}-04",
        f"maio de {ano}": f"{ano}-05",
        f"junho de {ano}": f"{ano}-06",
        f"julho de {ano}": f"{ano}-07",
        f"agosto de {ano}": f"{ano}-08",
        f"setembro de {ano}": f"{ano}-09",
        f"outubro de {ano}": f"{ano}-10",
        f"novembro de {ano}": f"{ano}-11",
        f"dezembro de {ano}": f"{ano}-12",
    }
    # retorna uma string com data no formato Y%-%m
    return conversor[mes_ano]


@task
def download_br_me_comex_stat(
    table_name: str,
    year_download: str,
) -> ZipFile:
    """
    This task creates directories to temporary input and output files
    and downloads the data from the source

    Args:
        mun for 'município'.
        table_name (str): the table name is the original name of the zip file with raw data from comex stat website

    Output:
        ZipFile: the zip file downloaded from comex stat website.
    """

    create_paths(
        path=comex_constants.PATH.value,
        table_name=table_name,
    )

    log("Paths created!")

    download_data(
        path=comex_constants.PATH.value,
        table_name=table_name,
        years_download=[year_download],
    )
    log("Data downloaded!")

    tm.sleep(10)


@task
def clean_br_me_comex_stat(
    path: str,
    table_type: str,
    table_name: str,
) -> pd.DataFrame:
    """
    This task reads a zip file donwload by the upstream task download_br_me_comex_stat and
    unpacks it into a csv file. Then, it cleans the data and returns a pandas dataframe partitioned by year and month
    or by year, month and sigla_uf.

    Args:
        table_name (str): the table name is the original name of the zip file with raw data from comex stat website


    Returns:
        pd.DataFrame: a partitioned standardized pandas dataframe
    """

    file_list = os.listdir(f"{path}{table_name}/input/")

    for file in file_list:
        if table_type == "mun":
            log(f"Doing file {file}")

            df = pd.read_csv(f"{path}{table_name}/input/{file}", sep=";")

            df = df.rename(columns=comex_constants.RENAME_MUN.value)

            log("Dataframe was renamed.")

            condicao = [
                ((df["sigla_uf"] == "SP") & (df["id_municipio"] < 3500000)),
                ((df["sigla_uf"] == "MS") & (df["id_municipio"] > 5000000)),
                ((df["sigla_uf"] == "GO") & (df["id_municipio"] > 5200000)),
                ((df["sigla_uf"] == "DF") & (df["id_municipio"] > 5300000)),
            ]

            valores = [
                df["id_municipio"] + 100000,
                df["id_municipio"] - 200000,
                df["id_municipio"] - 100000,
                df["id_municipio"] - 100000,
            ]

            df["id_municipio"] = np.select(
                condicao, valores, default=df["id_municipio"]
            )
            log("The id_municipio was column updated.")

            to_partitions(
                data=df,
                partition_columns=["ano", "mes", "sigla_uf"],
                savepath=comex_constants.PATH.value + table_name + "/output/",
            )

            log("Dataframe partitioned and saved")

            log("Starting dataframe validation:")
            validate_table(
                filename=file,
                dataframe=df,
                table_type="mun",
                path=comex_constants.PATH.value,
            )
            del df

        else:
            log(f"Doing file {file}")

            df = pd.read_csv(f"{path}{table_name}/input/{file}", sep=";")

            df = df.rename(columns=comex_constants.RENAME_NCM.value)
            log("Dataframe renamed")

            to_partitions(
                data=df,
                partition_columns=["ano", "mes"],
                savepath=comex_constants.PATH.value + table_name + "/output/",
            )
            log("Dataframe partitioned and saved")

            log("Starting dataframe validation:")
            validate_table(
                filename=file,
                dataframe=df,
                table_type="ncm",
                path=comex_constants.PATH.value,
            )
            del df

    return f"/tmp/br_me_comex_stat/{table_name}/output"
