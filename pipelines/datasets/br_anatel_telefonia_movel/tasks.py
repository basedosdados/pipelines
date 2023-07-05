# -*- coding: utf-8 -*-
"""
Tasks for br_anatel_telefonia_movel
"""

from prefect import task
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
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
def clean_csvs(mes_um, mes_dois):
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
    download_and_unzip(
        url=anatel_constants.URL.value, path=anatel_constants.INPUT_PATH.value
    )

    for anos in range(2019, 2024):
        print(f"Abrindo o arquivo:{mes_um}, {mes_dois}..")
        print("=" * 50)
        df = pd.read_csv(
            f"{anatel_constants.INPUT_PATH.value}Acessos_Telefonia_Movel_{anos}{mes_um}-{anos}{mes_dois}.csv",
            sep=";",
            encoding="utf-8",
        )
        print(f"Renomenado as colunas:")
        print("=" * 50)
        df.rename(columns=anatel_constants.RENAME.value, inplace=True)
        print(f"Removendo colunas desnecessárias: {mes_um}, {mes_dois}..")
        print("=" * 50)

        df.drop(["grupo_economico", "municipio", "ddd_chip"], axis=1, inplace=True)
        print(f"Agrupando por acessos: {mes_um}, {mes_dois}..")
        print("=" * 50)
        print(f"Ordenando-as: {mes_um}, {mes_dois}..")
        print("=" * 50)
        print(f"Tratando os dados: {mes_um}, {mes_dois}...")
        print("=" * 50)
        df["produto"] = df["produto"].str.lower()

        df["id_municipio"] = df["id_municipio"].astype(str)

        df["ddd"] = pd.to_numeric(df["ddd"], downcast="integer").astype(str)

        df["cnpj"] = df["cnpj"].astype(str)
        print(f"Ordenando-as: {mes_um}, {mes_dois}..")
        print("=" * 50)
        df = df[anatel_constants.ORDEM.value]

        print("=" * 50)

        print(f"Ordenando por ano e mes: {mes_um}, {mes_dois}...")

        to_partitions(
            df,
            partition_columns=["ano", "mes"],
            savepath=anatel_constants.OUTPUT_PATH.value,
        )

        return anatel_constants.OUTPUT_PATH.value


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def clean_csv_brasil():
    log("=" * 50)
    log("Download dos dados...")
    download_and_unzip(
        url=anatel_constants.URL.value, path=anatel_constants.INPUT_PATH.value
    )

    densidade = pd.read_csv(
        f"{anatel_constants.INPUT_PATH.value}Densidade_Telefonia_Movel",
        sep=";",
        encoding="utf-8",
    )
    densidade.rename(columns={"Nível Geográfico Densidade": "geografia"}, inplace=True)
    densidade_brasil = densidade[densidade["geografia"] == "Brasil"]
    densidade_brasil = densidade_brasil[["Ano", "Mês", "Densidade"]]
    densidade_brasil = densidade_brasil.rename(
        columns={"Ano": "ano", "Mês": "mes", "Densidade": "densidade"}
    )
    densidade_brasil["densidade"] = (
        densidade_brasil["densidade"].astype(str).str.replace(",", ".").astype(float)
    )
    densidade_brasil.to_csv(
        f"{anatel_constants.OUTPUT_PATH.value}densidade_brasil.csv",
        index=False,
        sep=",",
        encoding="utf-8",
        na_rep="",
    )
    return anatel_constants.OUTPUT_PATH.value


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def clean_csv_uf():
    log("=" * 50)
    log("Download dos dados...")
    download_and_unzip(
        url=anatel_constants.URL.value, path=anatel_constants.INPUT_PATH.value
    )

    densidade = pd.read_csv(
        f"{anatel_constants.INPUT_PATH.value}Densidade_Telefonia_Movel",
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
    densidade_uf.to_csv(
        f"{anatel_constants.OUTPUT_PATH.value}densidade_uf.csv",
        index=False,
        sep=",",
        encoding="utf-8",
        na_rep="",
    )
    return anatel_constants.OUTPUT_PATH.value


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def clean_csv_municipio():
    log("=" * 50)
    log("Download dos dados...")
    download_and_unzip(
        url=anatel_constants.URL.value, path=anatel_constants.INPUT_PATH.value
    )

    densidade = pd.read_csv(
        f"{anatel_constants.INPUT_PATH.value}Densidade_Telefonia_Movel",
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

    to_partitions(
        densidade_municipio,
        partition_columns=["ano", "mes"],
        savepath=anatel_constants.OUTPUT_PATH.value,
    )

    return anatel_constants.OUTPUT_PATH.value
