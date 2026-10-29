"""
Tasks for br_me_comex_stat.
"""

import os
import re
import time as tm
from collections.abc import Callable
from datetime import date
from zipfile import ZipFile

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
from prefect import task

from pipelines.datasets.br_me_comex_stat.constants import (
    DATASET_ID,
    TABLE_SPECS,
)
from pipelines.datasets.br_me_comex_stat.constants import (
    constants as comex_constants,
)
from pipelines.datasets.br_me_comex_stat.utils import (
    create_paths,
    download_data,
    validate_table,
)
from pipelines.utils.stage_dispatch import (
    ExtractAndLoad,
    SourceInspection,
    pipeline_factory,
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
    # pyrefly: ignore [missing-attribute]
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
    # pyrefly: ignore [bad-return]
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

    # pyrefly: ignore [bad-return]
    return f"/tmp/br_me_comex_stat/{table_name}/output"


# ──────────────────────────────────────────────────────────────────────────────
# As 4 tabelas (município/NCM x exportação/importação) — ver constants.py
#
# `get_latest_update` é a MESMA função pras 4 tabelas — a fonte de check é
# uma página de metadados única (`DOWNLOAD_LINK`), compartilhada, não por
# tabela (o flow antigo já comentava isso: "A fonte é uma só para as
# quatro tabelas"). Diferente de `br_ibge_ipca`, aqui o check é leve de
# verdade: `parse_last_date` só faz scrape de HTML de uma página de
# metadados, não baixa o dado bruto (~poucos KB de HTML) — não precisa da
# lógica de "baixar duas vezes" usada lá.
#
# `extract_load_data` precisa de fábrica por tabela (`table_name`/`table_type`
# variam). `clean_br_me_comex_stat` particiona por `ano/mes` (tabelas NCM)
# ou `ano/mes/sigla_uf` (tabelas de município, várias UFs por arquivo
# baixado) — `discover_partition_folders` (stage_dispatch.py) descobre as
# pastas-folha realmente escritas em disco, sem hardcoded as UFs
# presentes no arquivo.
# ──────────────────────────────────────────────────────────────────────────────


def br_me_comex_stat_get_latest_update() -> SourceInspection:
    last_date = parse_last_date(link=comex_constants.DOWNLOAD_LINK.value)
    # pyrefly: ignore [missing-attribute]
    year, month = last_date.split("-")
    return SourceInspection(reference_date=date(int(year), int(month), 1))


def make_extract_load_data(table_id: str) -> Callable[[dict], ExtractAndLoad]:
    spec = TABLE_SPECS[table_id]
    table_name = spec["table_name"]
    table_type = spec["table_type"]

    def extract_load_data(download_params: dict) -> ExtractAndLoad:
        ref = date.fromisoformat(download_params["reference_date"])
        year_download = f"{ref.year}-{ref.month:02d}"

        download_br_me_comex_stat(
            table_name=table_name, year_download=year_download
        )
        filepath = clean_br_me_comex_stat(
            path=comex_constants.PATH.value,
            table_type=table_type,
            table_name=table_name,
        )

        # `clean_br_me_comex_stat` está tipado (errado) como -> pd.DataFrame,
        # mas devolve `str` de verdade (mesma pendência do código original,
        # já marcada lá com `# pyrefly: ignore [bad-return]`).
        return ExtractAndLoad(
            # pyrefly: ignore [bad-argument-type]
            data_path=filepath,
        )

    return extract_load_data


make_pipeline = pipeline_factory(
    DATASET_ID,
    # Mesma função pras 4 tabelas -- a fonte de check é única,
    # compartilhada (ver banner acima).
    lambda _table_id: br_me_comex_stat_get_latest_update,
    make_extract_load_data,
    date_format="%Y-%m",
)
