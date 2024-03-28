# -*- coding: utf-8 -*-
"""
General purpose functions for the br_anatel_telefonia_movel project of the pipelines
"""
# pylint: disable=too-few-public-methods,invalid-name

import os
from io import BytesIO
from pathlib import Path
from urllib.request import urlopen
from zipfile import ZipFile
import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from pipelines.utils.utils import log
from pipelines.datasets.br_anatel_telefonia_movel.constants import (
    constants as anatel_constants,
)


def download(path):
    if not os.path.exists(path):
        os.makedirs(path)
    options = webdriver.ChromeOptions()
    # https://github.com/SeleniumHQ/selenium/issues/11637
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
    driver.get('https://dados.gov.br/dados/conjuntos-dados/acessos-autorizadas-smp')

    driver.maximize_window()
    log('primeiro passo...')
    WebDriverWait(driver, 30).until(
                EC.visibility_of_element_located(
                    (By.XPATH, '/html/body/div/section/div/div[3]/div[2]/div[3]/div[2]/header/button')
                )
            ).click()

    log('segundo passo...')
    WebDriverWait(driver, 30).until(
                EC.visibility_of_element_located(
                    (By.XPATH, '/html/body/div/section/div/div[3]/div[2]/div[3]/div[2]/div/div[1]/div[2]/div[2]/div/button')
                )
            ).click()
    log('time...')
    time.sleep(150)
    log("Terminou o time...")
    log(os.listdir(path))

def descompactar_arquivo():
    download(path = anatel_constants.INPUT_PATH.value)
    # Obtenha o nome do arquivo ZIP baixado
    zip_file_path = os.path.join(anatel_constants.INPUT_PATH.value, 'acessos_telefonia_movel.zip')
    time.sleep(300)
    print(os.listdir(anatel_constants.INPUT_PATH.value))
    try:
        with ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall(anatel_constants.INPUT_PATH.value)

    except Exception as e:
            print(f"Erro ao baixar ou extrair o arquivo ZIP: {str(e)}")

def to_partitions_microdados(
    data: pd.DataFrame, partition_columns: list[str], savepath: str
):
    """Save data in to hive patitions schema, given a dataframe and a list of partition columns.
    Args:
        data (pandas.core.frame.DataFrame): Dataframe to be partitioned.
        partition_columns (list): List of columns to be used as partitions.
        savepath (str, pathlib.PosixPath): folder path to save the partitions
    Exemple:
        data = {
            "ano": [2020, 2021, 2020, 2021, 2020, 2021, 2021,2025],
            "mes": [1, 2, 3, 4, 5, 6, 6,9],
            "sigla_uf": ["SP", "SP", "RJ", "RJ", "PR", "PR", "PR","PR"],
            "dado": ["a", "b", "c", "d", "e", "f", "g",'h'],
        }
        to_partitions(
            data=pd.DataFrame(data),
            partition_columns=['ano','mes','sigla_uf'],
            savepath='partitions/'
        )
    """

    if isinstance(data, (pd.core.frame.DataFrame)):
        savepath = Path(savepath)

        # create unique combinations between partition columns
        unique_combinations = (
            data[partition_columns]
            .drop_duplicates(subset=partition_columns)
            .to_dict(orient="records")
        )

        for filter_combination in unique_combinations:
            patitions_values = [
                f"{partition}={value}"
                for partition, value in filter_combination.items()
            ]

            # get filtered data
            df_filter = data.loc[
                data[filter_combination.keys()]
                .isin(filter_combination.values())
                .all(axis=1),
                :,
            ]
            df_filter = df_filter.drop(columns=partition_columns)

            # create folder tree
            filter_save_path = Path(savepath / "/".join(patitions_values))
            filter_save_path.mkdir(parents=True, exist_ok=True)
            file_filter_save_path = Path(filter_save_path) / "data.csv"

            # append data to csv
            df_filter.to_csv(
                file_filter_save_path,
                index=False,
                mode="a",
                header=not file_filter_save_path.exists(),
            )
    else:
        raise BaseException("Data need to be a pandas DataFrame")
