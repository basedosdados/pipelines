# -*- coding: utf-8 -*-
"""
Tasks for br_bcb_agencia
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
from pipelines.datasets.br_bcb_agencia.constants import (
    constants as agencia_constants,
)
from pipelines.datasets.br_bcb_agencia.utils import (
    check_and_create_column,
    clean_column_names,
    clean_nome_municipio,
    create_cnpj_col,
    format_date,
    order_cols,
    read_file,
    remove_empty_spaces,
    remove_latin1_accents_from_df,
    remove_non_numeric_chars,
    rename_cols,
    str_to_title,
    strip_dataframe_columns,
)
from pipelines.utils.utils import log, to_partitions


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def extract_last_date(table_id: str) -> str:
    """This task will extract the last date of agencias or municipios ESTBAN table from
    BACEN website using selenium webdriver

    Args:
        table_id (str): Table identifier (agencia or municipio)

    Returns:
        str: The last release date of the table (Y%-m%) and the raw version (m%/%Y)
    """

    options = webdriver.ChromeOptions()

    # https://github.com/SeleniumHQ/selenium/issues/11637
    prefs = {
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
    log(f"using url {agencia_constants.AGENCIA_URL.value}")
    driver.get(agencia_constants.AGENCIA_URL.value)

    # select input field and click on it
    log(
        f" Searching for ---- {agencia_constants.CSS_INPUT_FIELD_DICT.value[table_id]} to click on"
    )
    input_field = WebDriverWait(driver, 20).until(
        EC.element_to_be_clickable(
            (
                By.CSS_SELECTOR,
                agencia_constants.CSS_INPUT_FIELD_DICT.value[table_id],
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

    # select date string
    raw_date = raw_date[0:7]

    # format it to %Y-%m
    date = dt.datetime.strptime(raw_date, "%m/%Y").strftime("%Y-%m")

    log(f"The most recent file date is ->>>> {date}")

    # quit driver session
    driver.quit()

    # return date, raw_date
    return date, raw_date


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def download_table(save_path: str, table_id: str, date: str) -> str:
    """This function downloads ESTBAN data from BACEN url using selenium webdriver
    and downloads it


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
    log(f"using url {agencia_constants.AGENCIA_URL.value}")

    driver = webdriver.Chrome(
        service=ChromeService(ChromeDriverManager().install()), options=options
    )

    driver.get(agencia_constants.AGENCIA_URL.value)
    driver.implicitly_wait(2)

    # select input field and send keys
    log("looking for input field and sending keys")
    input_field = WebDriverWait(driver, 20).until(
        EC.element_to_be_clickable(
            (
                By.CSS_SELECTOR,
                agencia_constants.CSS_INPUT_FIELD_DICT.value[table_id],
            )
        )
    )

    assert input_field.is_displayed()

    driver.execute_script("arguments[0].scrollIntoView();", input_field)
    input_field.click()
    input_field.send_keys(date)
    input_field.send_keys(Keys.ENTER)

    # click  button to download file
    sleep(2)
    download_button = WebDriverWait(driver, 20).until(
        EC.element_to_be_clickable(
            (
                By.CSS_SELECTOR,
                agencia_constants.CSS_DOWNLOAD_BUTTON.value[table_id],
            )
        )
    )
    download_button.click()
    # Sleep time to wait the download
    sleep(9)

    log(f"files {os.listdir(save_path)} were downloaded")

    return save_path


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def clean_data():
    """
    This task wrang the data from the downloaded files
    """

    ZIP_PATH = agencia_constants.ZIPFILE_PATH_AGENCIA.value
    INPUT_PATH = agencia_constants.INPUT_PATH_AGENCIA.value
    OUTPUT_PATH = agencia_constants.OUTPUT_PATH_AGENCIA.value

    log("building folders")
    if not os.path.exists(INPUT_PATH):
        os.makedirs(INPUT_PATH, exist_ok=True)

    if not os.path.exists(OUTPUT_PATH):
        os.makedirs(OUTPUT_PATH, exist_ok=True)

    zip_files = os.listdir(ZIP_PATH)
    log(f"unziping files ----> {zip_files}")
    for file in zip_files:
        log(f"Unziping file ----> : {file}")
        with zipfile.ZipFile(os.path.join(ZIP_PATH, file), "r") as z:
            z.extractall(INPUT_PATH)

    files = os.listdir(INPUT_PATH)
    log(f"The following files were unzipped: {files}")

    for file in files:
        # the files format change across the year
        if file.endswith(".xls") or file.endswith(".xlsx"):
            file_path = os.path.join(INPUT_PATH, file)
            df = read_file(file_path=file_path, file_name=file)
            # general columns stardantization
            df = clean_column_names(df)
            # rename columns
            df.rename(columns=rename_cols(), inplace=True)

            # fill left zeros with field range
            df["id_compe_bcb_agencia"] = (
                df["id_compe_bcb_agencia"].astype(str).str.zfill(4)
            )

            df["dv_do_cnpj"] = df["dv_do_cnpj"].astype(str).str.zfill(2)
            df["sequencial_cnpj"] = (
                df["sequencial_cnpj"].astype(str).str.zfill(4)
            )
            df["cnpj"] = df["cnpj"].astype(str).str.zfill(8)
            df["fone"] = df["fone"].astype(str).str.zfill(8)

            # check existence and create columns
            df = check_and_create_column(df, col_name="data_inicio")
            df = check_and_create_column(df, col_name="instituicao")
            df = check_and_create_column(df, col_name="id_instalacao")
            df = check_and_create_column(
                df, col_name="id_compe_bcb_instituicao"
            )
            df = check_and_create_column(df, col_name="id_compe_bcb_agencia")

            # drop ddd column thats going to be added later
            df.drop(columns=["ddd"], inplace=True)
            log("ddd removed")

            # some files doesnt have 'id_municipio', just 'nome'.
            # to add new ids is necessary to join by name
            # clean nome municipio
            log("cleaning municipio name")
            df = clean_nome_municipio(df, "nome")

            municipio = bd.read_sql(
                query="select * from `basedosdados.br_bd_diretorios_brasil.municipio`",
                from_file=True,
                # billing_project_id='basedosdados-dev'
            )
            municipio = municipio[["nome", "sigla_uf", "id_municipio", "ddd"]]

            municipio = clean_nome_municipio(municipio, "nome")
            # check if id_municipio already exists
            df["sigla_uf"] = df["sigla_uf"].str.strip()

            if "id_municipio" not in df.columns:
                # read municipio from bd datalake
                # join id_municipio to df
                df = pd.merge(
                    df,
                    municipio[["nome", "sigla_uf", "id_municipio", "ddd"]],
                    left_on=["nome", "sigla_uf"],
                    right_on=["nome", "sigla_uf"],
                    how="left",
                )

            # check if ddd already exists, if it doesnt, add it
            if "ddd" not in df.columns:
                df = pd.merge(
                    df,
                    municipio[["id_municipio", "ddd"]],
                    left_on=["id_municipio"],
                    right_on=["id_municipio"],
                    how="left",
                )

            # clean cep column
            df["cep"] = df["cep"].astype(str)
            df["cep"] = df["cep"].apply(remove_non_numeric_chars)

            # cnpj cleaning working
            df = create_cnpj_col(df)

            df["cnpj"] = df["cnpj"].apply(remove_non_numeric_chars)
            df["cnpj"] = df["cnpj"].apply(remove_empty_spaces)

            # select cols to title
            col_list_to_title = [
                "endereco",
                "complemento",
                "bairro",
                "nome_agencia",
            ]

            for col in col_list_to_title:
                str_to_title(df, column_name=col)
                log(f"column - {col} converted to title")

            # remove latin1 accents from all cols
            df = remove_latin1_accents_from_df(df)

            # format data_inicio
            df["data_inicio"] = df["data_inicio"].apply(format_date)

            # strip all df columns
            df = strip_dataframe_columns(df)

            # order columns
            df = df[order_cols()]
            log("cols ordered")

            to_partitions(
                data=df,
                savepath=OUTPUT_PATH,
                partition_columns=["ano", "mes"],
            )

    return OUTPUT_PATH
