# -*- coding: utf-8 -*-
"""
General purpose functions for the br_anatel_telefonia_movel project of the pipelines
"""
# pylint: disable=too-few-public-methods,invalid-name

import os
from zipfile import ZipFile
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from pipelines.utils.utils import log
from pipelines.utils.crawler_anatel.telefonia_movel.constants import (
    constants as anatel_constants,
)


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
                EC.visibility_of_element_located(
                    (By.XPATH, '/html/body/div/section/div/div[3]/div[2]/div[3]/div[2]/header/button')
                )
            ).click()
    WebDriverWait(driver, 60).until(
                EC.visibility_of_element_located(
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