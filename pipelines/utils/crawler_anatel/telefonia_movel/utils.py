"""
General purpose functions for the br_anatel_telefonia_movel project of the pipelines
"""

import gc
import os
from zipfile import ZipFile

import pandas as pd
import requests

from pipelines.utils.crawler_anatel.telefonia_movel.constants import (
    constants as anatel_constants,
)
from pipelines.utils.utils import log, to_partitions


def download_zip_file():
    response = requests.get(
        "https://dados.gov.br/api/publico/conjuntos-dados/acessos-autorizadas-smp",
        cookies=anatel_constants.COOEKIES.value,
        headers=anatel_constants.HEADERS.value,
    )
    r = response.json()
    for recurso in r["resources"]:
        if recurso["format"] == "CSV":
            download_url = recurso["url"]
            log(
                f"Baixando {download_url} em {anatel_constants.INPUT_PATH.value}"
            )

    with open(
        os.path.join(
            anatel_constants.INPUT_PATH.value, "acessos_telefonia_movel.zip"
        ),
        "wb",
    ) as file:
        response = requests.get(
            download_url,
            cookies=anatel_constants.COOEKIES.value,
            headers=anatel_constants.HEADERS.value,
        )
        file.write(response.content)

    log("Download concluído com sucesso!")


def unzip_file():
    os.makedirs(anatel_constants.INPUT_PATH.value, exist_ok=True)
    download_zip_file()
    zip_file_path = os.path.join(
        anatel_constants.INPUT_PATH.value, "acessos_telefonia_movel.zip"
    )

    try:
        with ZipFile(zip_file_path, "r") as zip_ref:
            zip_ref.extractall(anatel_constants.INPUT_PATH.value)
    except Exception as e:
        print(f"Erro ao baixar ou extrair o arquivo ZIP: {e!s}")

    os.remove(zip_file_path)
    gc.collect()


# ! TASK MICRODADOS
def clean_csv_microdados(ano, semestre, table_id):
    log("Download dos dados...")
    os.system(f"mkdir -p {anatel_constants.INPUT_PATH.value}")

    df = pd.read_csv(
        f"{anatel_constants.INPUT_PATH.value}Acessos_Telefonia_Movel_{ano}_{semestre}S.csv",
        sep=";",
        encoding="utf-8",
    )

    df = df.rename(columns=anatel_constants.RENAME_COLUMNS_MICRODADOS.value)

    df = df.drop(anatel_constants.DROP_COLUMNS_MICRODADOS.value, axis=1)

    df["produto"] = df["produto"].str.lower()

    df["id_municipio"] = df["id_municipio"].astype(str)

    df["ddd"] = pd.to_numeric(df["ddd"], downcast="integer").astype(str)

    df["cnpj"] = df["cnpj"].astype(str)

    df = df[anatel_constants.ORDER_COLUMNS_MICRODADOS.value]

    to_partitions(
        df,
        partition_columns=["ano", "mes"],
        savepath=anatel_constants.TABLES_OUTPUT_PATH.value[table_id],
    )


# ! TASK BRASIL
def clean_csv_brasil(table_id):
    densidade = pd.read_csv(
        f"{anatel_constants.INPUT_PATH.value}Densidade_Telefonia_Movel.csv",
        sep=";",
        encoding="utf-8",
    )

    densidade = densidade.rename(
        columns={"Nível Geográfico Densidade": "geografia"}
    )

    densidade_brasil = densidade[densidade["geografia"] == "Brasil"]

    densidade_brasil = densidade_brasil[
        anatel_constants.ORDER_COLUMNS_BRASIL.value
    ]

    densidade_brasil = densidade_brasil.rename(
        columns=anatel_constants.RENAME_COLUMNS_BRASIL.value
    )

    densidade_brasil["densidade"] = (
        densidade_brasil["densidade"]
        .astype(str)
        .str.replace(",", ".")
        .astype(float)
    )

    densidade_brasil.to_csv(
        f"{anatel_constants.TABLES_OUTPUT_PATH.value[table_id]}densidade_brasil.csv",
        index=False,
        sep=",",
        encoding="utf-8",
        na_rep="",
    )


# ! TASK UF
def clean_csv_uf(table_id):
    densidade = pd.read_csv(
        f"{anatel_constants.INPUT_PATH.value}Densidade_Telefonia_Movel.csv",
        sep=";",
        encoding="utf-8",
    )

    densidade = densidade.rename(
        columns={"Nível Geográfico Densidade": "geografia"}
    )

    densidade_uf = densidade[densidade["geografia"] == "UF"]

    densidade_uf = densidade_uf[anatel_constants.ORDER_COLUMNS_UF.value]

    densidade_uf = densidade_uf.rename(
        columns=anatel_constants.RENAME_COLUMNS_UF.value
    )

    densidade_uf["densidade"] = (
        densidade_uf["densidade"]
        .astype(str)
        .str.replace(",", ".")
        .astype(float)
    )
    densidade_uf.to_csv(
        f"{anatel_constants.TABLES_OUTPUT_PATH.value[table_id]}densidade_uf.csv",
        index=False,
        sep=",",
        encoding="utf-8",
        na_rep="",
    )


# ! TASK MUNICIPIO
def clean_csv_municipio(table_id):
    densidade = pd.read_csv(
        f"{anatel_constants.INPUT_PATH.value}Densidade_Telefonia_Movel.csv",
        sep=";",
        encoding="utf-8",
    )
    densidade = densidade.rename(
        columns={"Nível Geográfico Densidade": "geografia"}
    )
    densidade_municipio = densidade[densidade["geografia"] == "Municipio"]
    densidade_municipio = densidade_municipio[
        anatel_constants.ORDER_COLUMNS_MUNICIPIO.value
    ]
    densidade_municipio = densidade_municipio.rename(
        columns=anatel_constants.RENAME_COLUMNS_MUNICIPIO.value
    )
    densidade_municipio["densidade"] = (
        densidade_municipio["densidade"]
        .astype(str)
        .str.replace(",", ".")
        .astype(float)
    )
    densidade_municipio.to_csv(
        f"{anatel_constants.TABLES_OUTPUT_PATH.value[table_id]}densidade_municipio.csv",
        index=False,
        sep=",",
        encoding="utf-8",
        na_rep="",
    )


def get_year():
    lista = []
    for x in os.listdir(anatel_constants.INPUT_PATH.value):
        parts = x.split("_")
        if len(parts) > 3:
            x = parts[3]
            if len(x) == 4:
                lista.append(x)

    max_year = max(lista)
    log(f"Ano máximo: {max_year}")
    return max_year
