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
    anos = [2019, 2020, 2021, 2022, 2023]
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

    for anos in range(2019, 2024):
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
        log(f"Agrupando por acessos: {anos}, {mes_um}, {mes_dois}..")
        log("=" * 50)
        log(f"Ordenando-as: {anos}, {mes_um}, {mes_dois}..")
        log("=" * 50)
        log(f"Tratando os dados: {anos}, {mes_um}, {mes_dois}...")
        log("=" * 50)
        df["produto"] = df["produto"].str.lower()

        df["id_municipio"] = df["id_municipio"].astype(str)

        df["ddd"] = pd.to_numeric(df["ddd"], downcast="integer").astype(str)

        df["cnpj"] = df["cnpj"].astype(str)
        log(f"Ordenando-as: {anos}, {mes_um}, {mes_dois}..")
        log("=" * 50)
        df = df[anatel_constants.ORDEM.value]

        log("=" * 50)

        log(f"Ordenando por ano e mes: {anos}, {mes_um}, {mes_dois}...")

        to_partitions(
            df,
            partition_columns=["ano", "mes"],
            savepath=anatel_constants.OUTPUT_PATH.value,
        )

    return anatel_constants.OUTPUT_PATH.value
