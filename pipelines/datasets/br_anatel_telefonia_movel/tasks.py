# -*- coding: utf-8 -*-
"""
Tasks for br_anatel_telefonia_movel
"""

from prefect import task
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
from pipelines.constants import constants
from pipelines.datasets.br_anatel_telefonia_movel.utils import download_and_unzip
from pipelines.datasets.br_anatel_telefonia_movel.constants import (
    constants as anatel_constants,
)
from pipelines.utils.utils import to_partitions, log


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def clean_csv_microdados(anos, mes_um, mes_dois):
    """
    -------
    Reads and cleans all CSV files in the '/tmp/data/input/' directory.
    1. Rename the columns
    2. Drops the columns Grupo_economico, Municipio and Ddd_chip
    3. groups all variables by "accesses"
    4. str.lower() on product column

    -------
    Returns:
    -------

    pd.DataFrame
        The cleaned DataFrame with the following columns:
            'ano', 'mes', 'sigla_uf', 'id_municipio', 'ddd', 'cnpj', 'empresa', 'porte_empresa', 'tecnologia',
            'sinal', 'modalidade', 'pessoa', 'produto', 'acessos'
    """
    log("=" * 50)
    log("Download dos dados...")
    log(anatel_constants.URL.value)
    download_and_unzip(
        url=anatel_constants.URL.value, path=anatel_constants.INPUT_PATH.value
    )
    log(f"Abrindo o arquivo:{anos}, {mes_um}, {mes_dois}..")
    log("=" * 50)
    df = pd.read_csv(
        f"{anatel_constants.INPUT_PATH.value}Acessos_Telefonia_Movel_{anos}{mes_um}-{anos}{mes_dois}.csv",
        sep=";",
        encoding="utf-8",
    )
    log(f"Renomenado as colunas:")
    log("=" * 50)
    df.rename(columns=anatel_constants.RENAME.value, inplace=True)
    log(f"Removendo colunas desnecessárias: {anos}, {mes_um}, {mes_dois}..")
    log("=" * 50)

    df.drop(["grupo_economico", "municipio", "ddd_chip"], axis=1, inplace=True)

    log(f"Tratando os dados: {anos}, {mes_um}, {mes_dois}...")
    log("=" * 50)

    df["produto"] = df["produto"].str.lower()

    df["id_municipio"] = df["id_municipio"].astype(str)

    df["ddd"] = pd.to_numeric(df["ddd"], downcast="integer").astype(str)

    df["cnpj"] = df["cnpj"].astype(str)

    df = df[anatel_constants.ORDEM.value]

    to_partitions(
        df,
        partition_columns=["ano", "mes"],
        savepath=anatel_constants.OUTPUT_PATH_MICRODADOS.value,
    )

    return anatel_constants.OUTPUT_PATH_MICRODADOS.value


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def clean_csv_brasil():
    log("=" * 50)
    log("Abrindo os dados do Brasil...")
    log(anatel_constants.URL.value)
    log("=" * 50)
    log(anatel_constants.INPUT_PATH.value)

    densidade = pd.read_csv(
        f"{anatel_constants.INPUT_PATH.value}Densidade_Telefonia_Movel.csv",
        sep=";",
        encoding="utf-8",
    )
    log("=" * 50)
    log(densidade.head())
    log("=" * 50)
    densidade.rename(columns={"Nível Geográfico Densidade": "geografia"}, inplace=True)
    densidade_brasil = densidade[densidade["geografia"] == "Brasil"]
    densidade_brasil = densidade_brasil[["Ano", "Mês", "Densidade"]]
    densidade_brasil = densidade_brasil.rename(
        columns={"Ano": "ano", "Mês": "mes", "Densidade": "densidade"}
    )
    densidade_brasil["densidade"] = (
        densidade_brasil["densidade"].astype(str).str.replace(",", ".").astype(float)
    )
    log("=" * 50)
    log(densidade_brasil.head())
    log("=" * 50)

    os.system(f"mkdir -p {anatel_constants.OUTPUT_PATH_BRASIL.value}")

    densidade_brasil.to_csv(
        f"{anatel_constants.OUTPUT_PATH_BRASIL.value}densidade_brasil.csv",
        index=False,
        sep=",",
        encoding="utf-8",
        na_rep="",
    )
    return anatel_constants.OUTPUT_PATH_BRASIL.value


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def clean_csv_uf():
    densidade = pd.read_csv(
        f"{anatel_constants.INPUT_PATH.value}Densidade_Telefonia_Movel.csv",
        sep=";",
        encoding="utf-8",
    )
    densidade.rename(columns={"Nível Geográfico Densidade": "geografia"}, inplace=True)
    densidade_uf = densidade[densidade["geografia"] == "UF"]
    densidade_uf = densidade_uf[["Ano", "Mês", "UF", "Densidade"]]
    densidade_uf = densidade_uf.rename(
        columns={"Ano": "ano", "Mês": "mes", "UF": "sigla_uf", "Densidade": "densidade"}
    )
    densidade_uf["densidade"] = (
        densidade_uf["densidade"].astype(str).str.replace(",", ".").astype(float)
    )
    os.system(f"mkdir -p {anatel_constants.OUTPUT_PATH_UF.value}")
    densidade_uf.to_csv(
        f"{anatel_constants.OUTPUT_PATH_UF.value}densidade_uf.csv",
        index=False,
        sep=",",
        encoding="utf-8",
        na_rep="",
    )
    return anatel_constants.OUTPUT_PATH_UF.value


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def clean_csv_municipio():
    densidade = pd.read_csv(
        f"{anatel_constants.INPUT_PATH.value}Densidade_Telefonia_Movel.csv",
        sep=";",
        encoding="utf-8",
    )

    densidade.rename(columns={"Nível Geográfico Densidade": "geografia"}, inplace=True)

    densidade_municipio = densidade[densidade["geografia"] == "Municipio"]

    densidade_municipio = densidade_municipio[
        ["Ano", "Mês", "UF", "Código IBGE", "Densidade"]
    ]

    densidade_municipio = densidade_municipio.rename(
        columns={
            "Ano": "ano",
            "Mês": "mes",
            "UF": "sigla_uf",
            "Código IBGE Município": "id_municipio",
            "Densidade": "densidade",
        }
    )
    densidade_municipio["densidade"] = (
        densidade_municipio["densidade"].astype(str).str.replace(",", ".").astype(float)
    )

    os.system(f"mkdir -p {anatel_constants.OUTPUT_PATH_MUNICIPIO.value}")

    to_partitions(
        densidade_municipio,
        partition_columns=["ano", "mes"],
        savepath=anatel_constants.OUTPUT_PATH_MUNICIPIO.value,
    )

    return anatel_constants.OUTPUT_PATH_MUNICIPIO.value
