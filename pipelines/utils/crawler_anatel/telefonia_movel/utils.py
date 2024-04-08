# -*- coding: utf-8 -*-
"""
General purpose functions for the br_anatel_telefonia_movel project of the pipelines
"""
# pylint: disable=too-few-public-methods,invalid-name

import os
from zipfile import ZipFile
import time
import pandas as pd
import numpy as np
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from pipelines.utils.utils import log
from pipelines.utils.crawler_anatel.telefonia_movel.constants import (
    constants as anatel_constants,
)
from pipelines.utils.utils import log, to_partitions


def download_zip_file(path):
    if not os.path.exists(path):
        os.makedirs(path)
    options = webdriver.ChromeOptions()
    prefs = {
        "download.default_directory": path,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
    }
    options.add_experimental_option(
        "prefs",
        prefs,
    )
    options.add_argument("--headless=new")
    options.add_argument("--test-type")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-first-run")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--no-default-browser-check")
    options.add_argument("--ignore-certificate-errors")
    options.add_argument("--start-maximized")
    options.add_argument(
        "user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
    )

    driver = webdriver.Chrome(options=options)
    driver.get(anatel_constants.URL.value)
    driver.maximize_window()
    WebDriverWait(driver, 60).until(
                EC.element_to_be_clickable(
                    (By.XPATH, '/html/body/div/section/div/div[3]/div[2]/div[3]/div[2]/header/button')
                )
            ).click()
    WebDriverWait(driver, 60).until(
                EC.element_to_be_clickable(
                    (By.XPATH, '/html/body/div/section/div/div[3]/div[2]/div[3]/div[2]/div/div[1]/div[2]/div[2]/div/button')
                )
            ).click()
    time.sleep(150)
    log(os.listdir(path))

def unzip_file():
    download_zip_file(path = anatel_constants.INPUT_PATH.value)
    zip_file_path = os.path.join(anatel_constants.INPUT_PATH.value, 'acessos_telefonia_movel.zip')
    time.sleep(300)
    try:
        with ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall(anatel_constants.INPUT_PATH.value)

    except Exception as e:
            print(f"Erro ao baixar ou extrair o arquivo ZIP: {str(e)}")

# ! TASK MICRODADOS
def clean_csv_microdados(ano, semestre, table_id):
    log("Download dos dados...")
    log(anatel_constants.URL.value)
    os.system(f"mkdir -p {anatel_constants.INPUT_PATH.value}")

    df = pd.read_csv(
        f"{anatel_constants.INPUT_PATH.value}Acessos_Telefonia_Movel_{ano}_{semestre}S.csv",
        sep=";",
        encoding="utf-8",
    )


    log("Renomeando as colunas:")


    df.rename(columns=anatel_constants.RENAME_COLUMNS_MICRODADOS.value, inplace=True)

    df.drop(anatel_constants.DROP_COLUMNS_MICRODADOS.value, axis=1, inplace=True)

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

    log("Abrindo os dados do Brasil...")

    densidade = pd.read_csv(
        f"{anatel_constants.INPUT_PATH.value}Densidade_Telefonia_Movel.csv",
        sep=";",
        encoding="utf-8",
    )

    densidade.rename(columns={"Nível Geográfico Densidade": "geografia"}, inplace=True)

    densidade_brasil = densidade[densidade["geografia"] == "Brasil"]

    densidade_brasil = densidade_brasil[anatel_constants.ORDER_COLUMNS_BRASIL.value]

    densidade_brasil = densidade_brasil.rename(
        columns=anatel_constants.RENAME_COLUMNS_BRASIL.value
    )

    densidade_brasil["densidade"] = (
        densidade_brasil["densidade"].astype(str).str.replace(",", ".").astype(float)
    )
    log("Salvando os dados do Brasil...")

    densidade_brasil.to_csv(
        f"{anatel_constants.TABLES_OUTPUT_PATH.value[table_id]}densidade_brasil.csv",
        index=False,
        sep=",",
        encoding="utf-8",
        na_rep="",
    )


# ! TASK UF
def clean_csv_uf(table_id):
    log("Abrindo os dados por UF...")
    densidade = pd.read_csv(
        f"{anatel_constants.INPUT_PATH.value}Densidade_Telefonia_Movel.csv",
        sep=";",
        encoding="utf-8",
    )

    densidade.rename(columns={"Nível Geográfico Densidade": "geografia"}, inplace=True)

    densidade_uf = densidade[densidade["geografia"] == "UF"]

    densidade_uf = densidade_uf[anatel_constants.ORDER_COLUMNS_UF.value]

    densidade_uf = densidade_uf.rename(
        columns=anatel_constants.RENAME_COLUMNS_UF.value
    )

    densidade_uf["densidade"] = (
        densidade_uf["densidade"].astype(str).str.replace(",", ".").astype(float)
    )
    log("Salvando dados por UF")
    densidade_uf.to_csv(
        f"{anatel_constants.TABLES_OUTPUT_PATH.value[table_id]}densidade_uf.csv",
        index=False,
        sep=",",
        encoding="utf-8",
        na_rep="",
    )


# ! TASK MUNICIPIO
def clean_csv_municipio(table_id):
    log("Abrindo os dados por município...")
    densidade = pd.read_csv(
        f"{anatel_constants.INPUT_PATH.value}Densidade_Telefonia_Movel.csv",
        sep=";",
        encoding="utf-8",
    )
    densidade.rename(columns={"Nível Geográfico Densidade": "geografia"}, inplace=True)
    densidade_municipio = densidade[densidade["geografia"] == "Municipio"]
    densidade_municipio = densidade_municipio[
        anatel_constants.ORDER_COLUMNS_MUNICIPIO.value
    ]
    densidade_municipio = densidade_municipio.rename(
        columns=anatel_constants.RENAME_COLUMNS_MUNICIPIO.value
    )
    densidade_municipio["densidade"] = (
        densidade_municipio["densidade"].astype(str).str.replace(",", ".").astype(float)
    )
    log("Salvando os dados por município...")
    densidade_municipio.to_csv(
        f"{anatel_constants.TABLES_OUTPUT_PATH.value[table_id]}densidade_municipio.csv",
        index=False,
        sep=",",
        encoding="utf-8",
        na_rep="",
    )