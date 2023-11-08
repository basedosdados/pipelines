# -*- coding: utf-8 -*-
"""
Tasks for dataset br_anatel_telefonia_movel
"""

import os
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from prefect import task

from pipelines.constants import constants
from pipelines.datasets.br_anatel_telefonia_movel.constants import (
    constants as anatel_constants,
)
from pipelines.datasets.br_anatel_telefonia_movel.utils import (
    to_partitions_microdados,
    data_url,
    download_and_unzip
)
from pipelines.utils.utils import  log

@task
def setting_data_url():
    meses = {
        "jan": "01",
        "fev": "02",
        "mar": "03",
        "abr": "04",
        "mai": "05",
        "jun": "06",
        "jul": "07",
        "ago": "08",
        "set": "09",
        "out": "10",
        "nov": "11",
        "dez": "12",
    }
    string_element = data_url()
    elemento_total = string_element[25:33]
    mes, ano = elemento_total.split("-")
    mes = meses[mes]
    data_total = f"{ano}-{mes}"
    log(data_total)

    return data_total


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def task_check_for_data():
    return setting_data_url()


# ! TASK MICRODADOS
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
    # Imprime uma linha de separação no log
    log("=" * 50)

    # Imprime a mensagem de download dos dados
    log("Download dos dados...")

    # Imprime a URL dos dados
    log(anatel_constants.URL.value)

    # Realiza o download e descompactação dos dados
    download_and_unzip(
        url=anatel_constants.URL.value, path=anatel_constants.INPUT_PATH.value
    )
    # Imprime a mensagem de abertura do arquivo
    log(f"Abrindo o arquivo:{anos}, {mes_um}, {mes_dois}..")

    # Imprime uma linha de separação no log
    log("=" * 50)

    # Lê o arquivo CSV contendo os dados
    df = pd.read_csv(
        f"{anatel_constants.INPUT_PATH.value}Acessos_Telefonia_Movel_{anos}{mes_um}-{anos}{mes_dois}.csv",
        sep=";",
        encoding="utf-8",
    )

    # Imprime a mensagem de renomeação das colunas
    log("Renomeando as colunas:")

    # Imprime uma linha de separação no log
    log("=" * 50)

    # Renomeia as colunas do DataFrame de acordo com as constantes definidas
    df.rename(columns=anatel_constants.RENAME.value, inplace=True)

    # Imprime a mensagem de remoção das colunas desnecessárias
    log(f"Removendo colunas desnecessárias: {anos}, {mes_um}, {mes_dois}..")

    # Imprime uma linha de separação no log
    log("=" * 50)

    # Remove as colunas desnecessárias do DataFrame
    df.drop(["grupo_economico", "municipio", "ddd_chip"], axis=1, inplace=True)

    # Imprime a mensagem de tratamento dos dados
    log(f"Tratando os dados: {anos}, {mes_um}, {mes_dois}...")

    # Imprime uma linha de separação no log
    log("=" * 50)

    # Converte os valores da coluna "produto" para letras minúsculas
    df["produto"] = df["produto"].str.lower()

    # Converte o tipo da coluna "id_municipio" para string
    df["id_municipio"] = df["id_municipio"].astype(str)

    # Converte o tipo da coluna "ddd" para numérico e, em seguida, para string
    df["ddd"] = pd.to_numeric(df["ddd"], downcast="integer").astype(str)

    # Converte o tipo da coluna "cnpj" para string
    df["cnpj"] = df["cnpj"].astype(str)

    # Ordena as colunas do DataFrame de acordo com as constantes definidas
    df = df[anatel_constants.ORDEM.value]

    # Divide o DataFrame em partições com base nas colunas "ano" e "mes" e salva os dados
    to_partitions_microdados(
        df,
        partition_columns=["ano", "mes"],
        savepath=anatel_constants.OUTPUT_PATH_MICRODADOS.value,
    )

    # Retorna o caminho de saída dos microdados
    return anatel_constants.OUTPUT_PATH_MICRODADOS.value


# ! TASK BRASIL
@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def clean_csv_brasil():
    # Imprime uma linha de separação no log
    log("=" * 50)

    # Imprime a mensagem de abertura dos dados do Brasil
    log("Abrindo os dados do Brasil...")

    # Imprime a URL dos dados
    log(anatel_constants.URL.value)

    # Imprime uma linha de separação no log
    log("=" * 50)

    # Imprime o caminho de entrada dos dados
    log(anatel_constants.INPUT_PATH.value)

    # Lê o arquivo CSV contendo os dados de densidade de telefonia móvel
    densidade = pd.read_csv(
        f"{anatel_constants.INPUT_PATH.value}Densidade_Telefonia_Movel.csv",
        sep=";",
        encoding="utf-8",
    )

    # Imprime uma linha de separação no log
    log("=" * 50)

    # Imprime as primeiras linhas do DataFrame de densidade
    log(densidade.head())

    # Imprime uma linha de separação no log
    log("=" * 50)

    # Renomeia a coluna "Nível Geográfico Densidade" para "geografia"
    densidade.rename(columns={"Nível Geográfico Densidade": "geografia"}, inplace=True)

    # Filtra os dados para obter apenas os dados do Brasil
    densidade_brasil = densidade[densidade["geografia"] == "Brasil"]

    # Seleciona apenas as colunas "Ano", "Mês" e "Densidade"
    densidade_brasil = densidade_brasil[["Ano", "Mês", "Densidade"]]

    # Renomeia as colunas para "ano", "mes" e "densidade"
    densidade_brasil = densidade_brasil.rename(
        columns={"Ano": "ano", "Mês": "mes", "Densidade": "densidade"}
    )

    # Converte o tipo da coluna "densidade" para float, substituindo vírgulas por pontos
    densidade_brasil["densidade"] = (
        densidade_brasil["densidade"].astype(str).str.replace(",", ".").astype(float)
    )

    # Imprime uma linha de separação no log
    log("=" * 50)

    # Imprime as primeiras linhas do DataFrame de densidade do Brasil
    log(densidade_brasil.head())

    # Imprime uma linha de separação no log
    log("=" * 50)

    # Cria um diretório de saída, se não existir
    os.system(f"mkdir -p {anatel_constants.OUTPUT_PATH_BRASIL.value}")

    # Salva os dados de densidade do Brasil em um arquivo CSV
    densidade_brasil.to_csv(
        f"{anatel_constants.OUTPUT_PATH_BRASIL.value}densidade_brasil.csv",
        index=False,
        sep=",",
        encoding="utf-8",
        na_rep="",
    )

    # Retorna o caminho de saída dos dados do Brasil
    return anatel_constants.OUTPUT_PATH_BRASIL.value


# ! TASK UF
@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def clean_csv_uf():
    # Lê o arquivo CSV contendo os dados de densidade de telefonia móvel
    densidade = pd.read_csv(
        f"{anatel_constants.INPUT_PATH.value}Densidade_Telefonia_Movel.csv",
        sep=";",
        encoding="utf-8",
    )

    # Renomeia a coluna "Nível Geográfico Densidade" para "geografia"
    densidade.rename(columns={"Nível Geográfico Densidade": "geografia"}, inplace=True)

    # Filtra os dados para obter apenas os dados das Unidades Federativas (UF)
    densidade_uf = densidade[densidade["geografia"] == "UF"]

    # Seleciona apenas as colunas "Ano", "Mês", "UF" e "Densidade"
    densidade_uf = densidade_uf[["Ano", "Mês", "UF", "Densidade"]]

    # Renomeia as colunas para "ano", "mes", "sigla_uf" e "densidade"
    densidade_uf = densidade_uf.rename(
        columns={"Ano": "ano", "Mês": "mes", "UF": "sigla_uf", "Densidade": "densidade"}
    )

    # Converte o tipo da coluna "densidade" para float, substituindo vírgulas por pontos
    densidade_uf["densidade"] = (
        densidade_uf["densidade"].astype(str).str.replace(",", ".").astype(float)
    )

    # Cria um diretório de saída, se não existir
    os.system(f"mkdir -p {anatel_constants.OUTPUT_PATH_UF.value}")

    # Salva os dados de densidade por UF em um arquivo CSV
    densidade_uf.to_csv(
        f"{anatel_constants.OUTPUT_PATH_UF.value}densidade_uf.csv",
        index=False,
        sep=",",
        encoding="utf-8",
        na_rep="",
    )

    # Retorna o caminho de saída dos dados de densidade por UF
    return anatel_constants.OUTPUT_PATH_UF.value


# ! TASK MUNICIPIO
@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def clean_csv_municipio():
    # Lê o arquivo CSV contendo os dados de densidade de telefonia móvel
    densidade = pd.read_csv(
        f"{anatel_constants.INPUT_PATH.value}Densidade_Telefonia_Movel.csv",
        sep=";",
        encoding="utf-8",
    )

    # Renomeia a coluna "Nível Geográfico Densidade" para "geografia"
    densidade.rename(columns={"Nível Geográfico Densidade": "geografia"}, inplace=True)

    # Filtra os dados para obter apenas os dados por município
    densidade_municipio = densidade[densidade["geografia"] == "Municipio"]

    # Seleciona apenas as colunas "Ano", "Mês", "UF", "Código IBGE" e "Densidade"
    densidade_municipio = densidade_municipio[
        ["Ano", "Mês", "UF", "Código IBGE", "Densidade"]
    ]

    # Renomeia as colunas para "ano", "mes", "sigla_uf", "id_municipio" e "densidade"
    densidade_municipio = densidade_municipio.rename(
        columns={
            "Ano": "ano",
            "Mês": "mes",
            "UF": "sigla_uf",
            "Código IBGE Município": "id_municipio",
            "Densidade": "densidade",
        }
    )

    # Converte o tipo da coluna "densidade" para float, substituindo vírgulas por pontos
    densidade_municipio["densidade"] = (
        densidade_municipio["densidade"].astype(str).str.replace(",", ".").astype(float)
    )

    # Cria um diretório de saída, se não existir
    os.system(f"mkdir -p {anatel_constants.OUTPUT_PATH_MUNICIPIO.value}")

    # Salva os dados de densidade por município em um arquivo CSV
    densidade_municipio.to_csv(
        f"{anatel_constants.OUTPUT_PATH_MUNICIPIO.value}densidade_municipio.csv",
        index=False,
        sep=",",
        encoding="utf-8",
        na_rep="",
    )

    # Retorna o caminho de saída dos dados de densidade por município
    return anatel_constants.OUTPUT_PATH_MUNICIPIO.value
