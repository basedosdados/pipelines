# -*- coding: utf-8 -*-
"""
General purpose functions for the br_stf_corte_aberta project
"""

from selenium import webdriver
import time
import os
import pandas as pd
import numpy as np
from datetime import datetime
from pipelines.datasets.br_stf_corte_aberta.constants import constants as stf_constants


def web_scrapping():
    if not os.path.exists(stf_constants.STF_INPUT.value):
        os.mkdir(stf_constants.STF_INPUT.value)
    options = webdriver.ChromeOptions()
    # https://github.com/SeleniumHQ/selenium/issues/11637
    prefs = {
        "download.default_directory": stf_constants.STF_INPUT.value,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
    }
    options.add_experimental_option(
        "prefs",
        prefs,
    )

    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--crash-dumps-dir=/tmp")
    options.add_argument("--remote-debugging-port=9222")

    driver = webdriver.Chrome(options=options)
    time.sleep(50)
    driver.get(stf_constants.STF_LINK.value)
    time.sleep(50)
    driver.maximize_window()
    time.sleep(50)
    driver.find_element("xpath", '//*[@id="EXPORT-BUTTON-2"]/button').click()
    time.sleep(50)


def read_csv():
    arquivos = os.listdir(stf_constants.STF_INPUT.value)
    for arquivo in arquivos:
        if arquivo.endswith(".csv"):
            df = pd.read_csv(stf_constants.STF_INPUT.value + arquivo, dtype=str)
    return df


def fix_columns_data(df):
    lista = ["Data de autuação", "Data da decisão", "Data baixa processo"]
    for x in lista:
        df[x] = df[x].astype(str).str[0:10]
        df[x] = (
            df[x].astype(str).str[6:10]
            + "-"
            + df[x].astype(str).str[3:5]
            + "-"
            + df[x].astype(str).str[0:2]
        )

    return df


def column_bool(df):
    df["Indicador de tramitação"] = (
        df["Indicador de tramitação"].replace("Não", "false").replace("Sim", "true")
    )
    return df


def rename_ordening_columns(df):
    df.rename(columns=stf_constants.RENAME.value, inplace=True)
    df = df[stf_constants.ORDEM.value]
    return df


def replace_columns(df):
    df["assunto_processo"] = df["assunto_processo"].apply(
        lambda x: str(x).replace("\r", " ")
    )
    df = df.apply(lambda x: x.replace("-", "").replace(np.nan, ""))
    return df


def partition_data(df: pd.DataFrame, column_name: list[str], output_directory: str):
    """
    Particiona os dados em subconjuntos de acordo com os valores únicos de uma coluna.
    Salva cada subconjunto em um arquivo CSV separado.
    df: DataFrame a ser particionado
    column_name: nome da coluna a ser usada para particionar os dados
    output_directory: diretório onde os arquivos CSV serão salvos
    """
    unique_values = df[column_name].unique()
    for value in unique_values:
        value_str = str(value)[:10]
        date_value = datetime.strptime(value_str, "%Y-%m-%d").date()

        formatted_value = date_value.strftime("%Y-%m-%d")

        partition_path = os.path.join(
            output_directory, f"{column_name}={formatted_value}"
        )

        if not os.path.exists(partition_path):
            os.makedirs(partition_path)

        df_partition = df[df[column_name] == value].copy()

        df_partition.drop([column_name], axis=1, inplace=True)

        csv_path = os.path.join(partition_path, "data.csv")
        # mode = "a" if os.path.exists(csv_path) else "w"
        df_partition.to_csv(
            csv_path,
            sep=",",
            index=False,
            encoding="utf-8",
            na_rep="",
        )
