# -*- coding: utf-8 -*-
"""
Tasks for br_inmet_bdmep
"""
import glob
import os
import re

import numpy as np
import pandas as pd
from prefect import task

from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select

from datetime import datetime, time, timedelta

from webdriver_manager.chrome import ChromeDriverManager

from pipelines.constants import constants
from pipelines.datasets.br_inmet_bdmep.constants import constants as inmet_constants
from pipelines.datasets.br_inmet_bdmep.utils import (
    download_inmet,
    get_clima_info,
    year_list,
)
from pipelines.utils.utils import log

# pylint: disable=C0103
@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def extract_last_date_from_source():
    """
    Extrai última data de atualização dos dados históricos do site do INMET.
    """
    padrao = r'(\d{2}/\d{2}/\d{4})'
    options = webdriver.ChromeOptions()

    # https://github.com/SeleniumHQ/selenium/issues/11637
    prefs = {
        "download.default_directory": "/tmp/",
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
    }
    options.add_experimental_option(
        "prefs",
        prefs,
    )

    options.add_argument("--headless")
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

    driver = webdriver.Chrome(
        service=ChromeService(ChromeDriverManager().install()), options=options
    )

    driver.get("https://portal.inmet.gov.br/dadoshistoricos/")

    elements = driver.find_elements(By.XPATH, '//*[@id="main"]/div/div/article')
    last_element = elements[-1].text
    last_date = re.findall(padrao, last_element)
    return datetime.strptime(last_date[-1], '%d/%m/%Y').date()

@task
def get_base_inmet(year: int) -> str:
    """
    Faz o download dos dados meteorológicos do INMET, processa-os e salva os dataframes resultantes em arquivos CSV.

    Retorna:
    - str: o caminho para o diretório que contém os arquivos CSV de saída.
    """
    log(f"Baixando os dados para o ano {year}.")

    download_inmet(year)
    log("Dados baixados.")

    files = glob.glob(os.path.join(f"/tmp/data/input/{year}/", "*.CSV"))

    base = pd.concat([get_clima_info(file) for file in files], ignore_index=True)

    # ordena as colunas
    ordem = inmet_constants.COLUMNS_ORDER.value
    base = base[ordem]

    # Salva o dataframe resultante em um arquivo CSV
    os.makedirs(os.path.join(f"/tmp/data/output/microdados/ano={year}"), exist_ok=True)
    name = os.path.join(
        f"/tmp/data/output/microdados/ano={year}/", f"microdados_{year}.csv"
    )
    base.to_csv(name, index=False)

    return "/tmp/data/output/microdados/"
