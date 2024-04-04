# -*- coding: utf-8 -*-
import os
import time
from zipfile import ZipFile
from pipelines.utils.utils import log
import pandas as pd
from pipelines.utils.crawler_anatel.banda_larga_fixa.constants import (
    constants as anatel_constants,
)
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


def download_zip_file(path):
    """
    Downloads a zip file from a specific URL and saves it to the given path.

    Args:
        path (str): The path where the downloaded zip file will be saved.

    Returns:
        None
    """
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
    time.sleep(300)

def unzip_file():
    download_zip_file(path=anatel_constants.INPUT_PATH.value)
    zip_file_path = os.path.join(anatel_constants.INPUT_PATH.value, 'acessos_banda_larga_fixa.zip')
    log(os.listdir(anatel_constants.INPUT_PATH.value))
    try:
        with ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall(anatel_constants.INPUT_PATH.value)
    except Exception as e:
        print(f"Erro ao baixar ou extrair o arquivo ZIP: {str(e)}")

def check_and_create_column(df: pd.DataFrame, col_name: str) -> pd.DataFrame:
    """
    # ! Verifique se existe uma coluna em um Pandas DataFrame. Caso contrário, crie uma nova coluna com o nome fornecido
    # ! e preenchê-lo com valores NaN. Se existir, não faça nada.

    # * Parâmetros:
    # ! df (Pandas DataFrame): O DataFrame a ser verificado.
    # ! col_name (str): O nome da coluna a ser verificada ou criada.

    # * Retorna:
    # ! Pandas DataFrame: O DataFrame modificado.
    """

    if col_name not in df.columns:
        df[col_name] = ""
    return df