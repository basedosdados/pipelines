# -*- coding: utf-8 -*-
"""
Tasks for br_bcb_estban
"""

import datetime as dt
import os
import zipfile
from datetime import timedelta
from time import sleep

import basedosdados as bd
import pandas as pd
from bs4 import BeautifulSoup
from prefect import task
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

from pipelines.constants import constants
from pipelines.datasets.br_bcb_estban.constants import (
    constants as br_bcb_estban_constants,
)
from pipelines.datasets.br_bcb_estban.utils import (
    cols_order_agencia,
    create_id_municipio,
    create_id_verbete_column,
    create_month_year_columns,
    download_and_unzip,
    extract_download_links,
    order_cols_municipio,
    parse_date,
    pre_cleaning_for_pivot_long_agencia,
    pre_cleaning_for_pivot_long_municipio,
    rename_columns_agencia,
    rename_columns_municipio,
    standardize_monetary_units,
    wide_to_long_agencia,
    wide_to_long_municipio,
)
from pipelines.utils.utils import clean_dataframe, log, to_partitions

# adicionar crawler que extrai url de download mais recente
# usar web driver pois o site é dinâmico

# 1. criar função que extrai as datas mais recente e o link da estban
# - usar crhomewebdriver
# - pega a data mais recente da api
# - send keys pro input field
# - handle error, se for beleza;
# - caso contrário morre e não atualiza;


@task
def extract_last_date(table: str) -> str:
    options = webdriver.ChromeOptions()

    # https://github.com/SeleniumHQ/selenium/issues/11637
    prefs = {
        # "download.default_directory": constants.TMP_DATA_DIR.value,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
    }

    options.add_experimental_option(
        "prefs",
        prefs,
    )

    options.add_argument("--headless=new")
    # NOTE: The traditional --headless, and since version 96, Chrome has a new headless mode that allows users to get the full browser functionality (even run extensions). Between versions 96 to 108 it was --headless=chrome, after version 109 --headless=new
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

    log("iniatilizing drivermanager")
    log(f"using url {br_bcb_estban_constants.ESTBAN_NEW_URL.value}")
    driver.get(br_bcb_estban_constants.ESTBAN_NEW_URL.value)

    # -- once the input field is clicked the website enables dropdown options
    # The most recent date is automatically select and given under the class ng-option ng-option-marked
    # TODO: add error handler to inform if it occurs

    # select input field and click on it
    log(
        f"searching for ---- {br_bcb_estban_constants.XPATH_INPUT_FIELD_DICT.value[table]} to click on"
    )
    input_field = WebDriverWait(driver, 20).until(
        EC.element_to_be_clickable(
            (
                By.CSS_SELECTOR,
                br_bcb_estban_constants.XPATH_INPUT_FIELD_DICT.value[table],
            )
        )
    )

    assert input_field.is_displayed()

    input_field.click()

    # parse source code
    page_source = driver.page_source

    # find class ng-option ng-option-marked
    soup = BeautifulSoup(page_source, "html.parser")
    raw_date = soup.find("div", class_="ng-option ng-option-marked").get_text()

    # format it to %Y-%m
    date = dt.datetime.strptime(raw_date, "%m/%Y").strftime("%Y-%m")

    log(f"The most recent file date is ->>>> {date}")

    # quit driver session
    driver.quit()

    return date, raw_date


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def extract_most_recent_date(xpath, url):
    # table date
    url_list = extract_download_links(url=url, xpath=xpath)

    dicionario_data_url = {parse_date(url): url for url in url_list}
    tupla_data_maxima_url = max(dicionario_data_url.items(), key=lambda x: x[0])
    data_maxima = tupla_data_maxima_url[0]
    link_data_maxima = tupla_data_maxima_url[1]

    return link_data_maxima, data_maxima


@task
def download_estban_selenium(save_path: str, table: str, date: str) -> str:
    """This function downloads ESTBAN data from BACEN url,
    unzip the csv files and return a path for the raw files


    Args:
        save_path (str): a temporary path to save the estban files

    Returns:
        str: The path to the estban files
    """

    if not os.path.exists(save_path):
        os.makedirs(save_path, exist_ok=True)

    options = webdriver.ChromeOptions()

    # https://github.com/SeleniumHQ/selenium/issues/11637
    prefs = {
        "download.default_directory": save_path,
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
    log("iniatilizing drivermanager")
    log(f"using url {br_bcb_estban_constants.ESTBAN_NEW_URL.value}")

    driver = webdriver.Chrome(
        service=ChromeService(ChromeDriverManager().install()), options=options
    )

    driver.get(br_bcb_estban_constants.ESTBAN_NEW_URL.value)
    driver.implicitly_wait(2)

    # select input field and send keys
    log("looking for input field and sending keys")
    input_field = WebDriverWait(driver, 20).until(
        EC.element_to_be_clickable(
            (
                By.CSS_SELECTOR,
                br_bcb_estban_constants.XPATH_INPUT_FIELD_DICT.value[table],
            )
        )
    )

    assert input_field.is_displayed()

    driver.execute_script("arguments[0].scrollIntoView();", input_field)
    input_field.click()
    input_field.send_keys(date)
    input_field.send_keys(Keys.ENTER)

    log("looking for input field and sending keys")
    # click  button to download file
    sleep(2)
    download_button = WebDriverWait(driver, 20).until(
        EC.element_to_be_clickable(
            (
                By.CSS_SELECTOR,
                br_bcb_estban_constants.XPATH_DOWNLOAD_BUTTON.value[table],
            )
        )
    )
    download_button.click()
    sleep(10)

    log("download task successfully !")
    log(f"files {os.listdir(save_path)} were downloaded")

    return save_path


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def get_id_municipio() -> pd.DataFrame:
    """get id municipio from basedosdados"""

    # TODO: change before sending to prod
    municipio = bd.read_sql(
        query="select * from `basedosdados.br_bd_diretorios_brasil.municipio`",
        # billing_project_id="basedosdados-dev",
        from_file=True,
    )

    municipio = municipio[["id_municipio_bcb", "id_municipio"]]

    municipio = dict(zip(municipio.id_municipio_bcb, municipio.id_municipio))
    log("municipio dataset successfully downloaded!")
    return municipio


# 2. clean data
@task
def cleaning_municipios_data(municipio: str) -> str:
    """Perform data cleaning operations with estban municipios data

    Args:
        df: a raw municipios estban dataset

    Returns:
        df: a standardized partitioned estban dataset
    """
    ZIP_PATH = br_bcb_estban_constants.ZIPFILE_PATH_MUNICIPIO.value
    INPUT_PATH = br_bcb_estban_constants.INPUT_PATH_MUNICIPIO.value
    OUTPUT_PATH = br_bcb_estban_constants.OUTPUT_PATH_MUNICIPIO.value

    if not os.path.exists(OUTPUT_PATH):
        os.makedirs(INPUT_PATH, exist_ok=True)

    if not os.path.exists(OUTPUT_PATH):
        os.makedirs(INPUT_PATH, exist_ok=True)

    zip_files = os.listdir(ZIP_PATH)
    for file in zip_files:
        log(f"Unziping file ----> : {file}")
        with zipfile.ZipFile(os.path.join(ZIP_PATH, file), "r") as z:
            z.extractall(INPUT_PATH)

    # todo: PROBLEM IF multiple zipfiles are present:
    # for file in os.listdir(path):
    #    if file.endswith(".zip"):
    #        os.remove(os.path.join(path, file))
    #    log(f"deleting zip files ----> : {file}")

    csv_files = os.listdir(INPUT_PATH)

    for file in csv_files:
        log(f"the file being cleaned is:{file}")

        file_path = os.path.join(INPUT_PATH, file)

        log(f"building {file_path}")

        df = pd.read_csv(
            file_path,
            sep=";",
            index_col=None,
            encoding="latin-1",
            skipfooter=2,
            skiprows=2,
            dtype={"CNPJ": str, "CODMUN": str},
        )

        log("reading file")
        df = rename_columns_municipio(df)

        log("renaming columns")
        df = clean_dataframe(df)

        log("cleaning dataframe")
        df = create_id_municipio(df, municipio)

        log("creating id municipio")
        df = pre_cleaning_for_pivot_long_municipio(df)

        log("pre cleaning for pivot long")
        df = wide_to_long_municipio(df)

        log("wide to long")
        df = standardize_monetary_units(
            df, date_column="data_base", value_column="valor"
        )

        log("standardizing monetary units")
        df = create_id_verbete_column(df, column_name="id_verbete")

        log("creating id verbete column")
        df = create_month_year_columns(df, date_column="data_base")

        log("creating month year columns")
        df = order_cols_municipio(df)
        # save df

        log("saving and doing partition")
        # 3. build and save partition
        to_partitions(
            df,
            partition_columns=["ano", "mes", "sigla_uf"],
            savepath=OUTPUT_PATH,
        )

        del df

    return OUTPUT_PATH


# 2. clean data
@task
def cleaning_agencias_data(municipio):
    """Perform data cleaning operations with estban municipios data

    Args:
        df: a raw municipios estban dataset

    Returns:
        df: a standardized partitioned estban dataset
    """
    ZIP_PATH = br_bcb_estban_constants.ZIPFILE_PATH_AGENCIA.value
    INPUT_PATH = br_bcb_estban_constants.INPUT_PATH_AGENCIA.value
    OUTPUT_PATH = br_bcb_estban_constants.OUTPUT_PATH_AGENCIA.value

    if not os.path.exists(OUTPUT_PATH):
        os.makedirs(INPUT_PATH, exist_ok=True)

    if not os.path.exists(OUTPUT_PATH):
        os.makedirs(INPUT_PATH, exist_ok=True)

    zip_files = os.listdir(ZIP_PATH)
    for file in zip_files:
        log(f"Unziping file ----> : {file}")
        with zipfile.ZipFile(os.path.join(ZIP_PATH, file), "r") as z:
            z.extractall(INPUT_PATH)

    csv_files = os.listdir(INPUT_PATH)

    for file in csv_files:
        log(f"the file being cleaned is:{file}")

        file_path = os.path.join(INPUT_PATH, file)

        log(f"building {file_path}")

        df = pd.read_csv(
            file_path,
            sep=";",
            index_col=None,
            encoding="latin-1",
            skipfooter=2,
            skiprows=2,
            dtype={"CNPJ": str, "CODMUN": str},
        )

        log("reading file")
        df = rename_columns_agencia(df)

        log("renaming columns")
        # see the behavior of the function
        df = clean_dataframe(df)

        log("cleaning dataframe")
        df = create_id_municipio(df, municipio)

        log("creating id municipio")
        df = pre_cleaning_for_pivot_long_agencia(df)

        log("pre cleaning for pivot long")
        df = wide_to_long_agencia(df)

        log("wide to long")
        df = standardize_monetary_units(
            df, date_column="data_base", value_column="valor"
        )
        log("standardizing monetary units")
        df = create_id_verbete_column(df, column_name="id_verbete")

        log("creating id verbete column")
        df = create_month_year_columns(df, date_column="data_base")

        log("creating month year columns")
        df = cols_order_agencia(df)

        to_partitions(
            df,
            partition_columns=["ano", "mes", "sigla_uf"],
            savepath=OUTPUT_PATH,
        )

        del df

    return OUTPUT_PATH
