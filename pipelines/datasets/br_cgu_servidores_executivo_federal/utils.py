# -*- coding: utf-8 -*-
"""
General purpose functions for the br_cgu_servidores_executivo_federal project
"""

import datetime
import io
import os
import zipfile

import pandas as pd
import requests

from pipelines.datasets.br_cgu_servidores_executivo_federal.constants import constants
from pipelines.utils.apply_architecture_to_dataframe.utils import (
    read_architecture_table,
    rename_columns,
)
from pipelines.utils.utils import log


from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from tqdm import tqdm
from webdriver_manager.chrome import ChromeDriverManager

def make_url(sheet_name: str, date: datetime.date) -> str:
    if date.year <= 2019 and sheet_name not in [
        "Militares",
        "Servidores_BACEN",
        "Servidores_SIAPE",
    ]:
        raise ValueError(f"Invalid {sheet_name} for {date.year}, {date.month}")

    month = f"0{date.month}" if date.month < 10 else date.month
    file = f"{date.year}{month}_{sheet_name}"

    return f"{constants.URL.value}/{file}"


def build_urls(sheet_name: str, dates: list[datetime.date]):
    return [{"url": make_url(sheet_name, date), "date": date} for date in dates]


def download_zip_files_for_sheet(sheet_name: str, sheet_urls: list):
    sheet_input_folder = f"{constants.INPUT.value}/{sheet_name}"

    if not os.path.exists(sheet_input_folder):
        os.mkdir(sheet_input_folder)

    session = requests.Session()
    session.mount("https://", requests.adapters.HTTPAdapter(max_retries=3))  # type: ignore

    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.93 Safari/537.36",
    }

    log(f"Starting download for {sheet_name=}")

    for sheet_info in sheet_urls:

        href = sheet_info["url"]
        date = sheet_info["date"]
        year = date.year
        month = date.month
        try:
            response = requests.get(href, timeout=1000, stream=True, headers=headers)
            z = zipfile.ZipFile(io.BytesIO(response.content))
            z.extractall(f"{sheet_input_folder}/{year}-{month}")
        except:
            log(f'Essa url ainda não está disponível -->  {href}')

    log(f"Finished download for {sheet_name=}")


def get_csv_file_by_table_name_and_date(table_name: str, date: datetime.date) -> str:
    if table_name in [
        "cadastro_aposentados",
        "cadastro_pensionistas",
        "cadastro_servidores",
        "cadastro_reserva_reforma_militares",
    ]:
        pattern = "Cadastro"
    elif table_name == "remuneracao":
        pattern = "Remuneracao"
    elif table_name == "observacoes":
        pattern = "Observacoes"
    elif table_name == "afastamentos":
        pattern = "Afastamentos"
    else:
        raise ValueError(f"Not found file pattern for {table_name=}, {date=}")

    month = f"0{date.month}" if date.month < 10 else date.month

    path = f"{date.year}{month}_{pattern}.csv"

    return path


def get_source(table_name: str, source: str) -> str:
    ORIGINS = {
        "cadastro_aposentados": {
            "Aposentados_BACEN": "BACEN",
            "Aposentados_SIAPE": "SIAPE",
        },
        "cadastro_pensionistas": {
            "Pensionistas_SIAPE": "SIAPE",
            "Pensionistas_DEFESA": "Defesa",
            "Pensionistas_BACEN": "BACEN",
        },
        "cadastro_servidores": {
            "Servidores_BACEN": "BACEN",
            "Servidores_SIAPE": "SIAPE",
            "Militares": "Militares",
        },
        "cadastro_reserva_reforma_militares": {
            "Reserva_Reforma_Militares": "Reserva Reforma Militares"
        },
        "remuneracao": {
            "Militares": "Militares",
            "Pensionistas_BACEN": "Pensionistas BACEN",
            "Pensionistas_DEFESA": "Pensionistas DEFESA",
            "Reserva_Reforma_Militares": "Reserva Reforma Militares",
            "Servidores_BACEN": "Servidores BACEN",
            "Servidores_SIAPE": "Servidores SIAPE",
        },
        "afastamentos": {"Servidores_BACEN": "BACEN", "Servidores_SIAPE": "SIAPE"},
        "observacoes": {
            "Aposentados_BACEN": "Aposentados BACEN",
            "Aposentados_SIAPE": "Aposentados SIAPE",
            "Militares": "Militares",
            "Pensionistas_BACEN": "Pensionistas BACEN",
            "Pensionistas_DEFESA": "Pensionistas DEFESA",
            "Pensionistas_SIAPE": "Pensionistas SIAPE",
            "Reserva_Reforma_Militares": "Reserva Reforma Militares",
            "Servidores_BACEN": "Servidores BACEN",
            "Servidores_SIAPE": "Servidores SIAPE",
        },
    }

    return ORIGINS[table_name][source]


def read_and_clean_csv(
    table_name: str, source: str, date: datetime.date
) -> pd.DataFrame:
    csv_path = get_csv_file_by_table_name_and_date(table_name, date)

    path = f"{constants.INPUT.value}/{source}/{date.year}-{date.month}/{csv_path}"

    log(f"Reading {table_name=}, {source=}, {date=} {path=}")

    if not os.path.exists(path):
        log(f"File {path=} dont exists")
        return pd.DataFrame()

    df = pd.read_csv(
        path,
        sep=";",
        encoding="latin-1",
    ).rename(
        columns=lambda col: col.replace("\x96 ", "")
    )  # some csv files contains \x96 in header lines

    url_architecture = constants.ARCH.value[table_name]

    df_architecture = read_architecture_table(url_architecture)

    df = rename_columns(df, df_architecture)

    df["ano"] = date.year
    df["mes"] = date.month

    if "origem" in df_architecture["name"].to_list():
        df["origem"] = get_source(table_name, source)

    return df


def process_table(table_info: dict) -> tuple[str, pd.DataFrame]:
    table_name: str = table_info["table_name"]
    sources: list[str] = table_info["sources"]
    dates: list[datetime.date] = table_info["dates"]

    def read_csv_by_source(source: str):
        dfs = [read_and_clean_csv(table_name, source, date) for date in dates]

        return pd.concat(dfs)

    log(f"Processing {table_name=}, {sources=}")

    return (
        table_name,
        pd.concat([read_csv_by_source(source) for source in sources]),
    )




def extract_dates(table ="Servidores_SIAPE") -> pd.DataFrame:
    """
    Extrai datas e URLs de download do site do Portal da Transparência e retorna um DataFrame.
    """
    if not os.path.exists("/tmp/data/br_cgu_servidores_executivo_federal/tmp"):
        os.makedirs("/tmp/data/br_cgu_servidores_executivo_federal/tmp", exist_ok=True)
    options = webdriver.ChromeOptions()
    prefs = {
        "download.default_directory": "/tmp/data/br_cgu_servidores_executivo_federal/tmp",
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

    if table == "Servidores_SIAPE":
        driver.get("http://portaldatransparencia.gov.br/download-de-dados/servidores/")
        driver.implicitly_wait(10)

    page_source = driver.page_source
    BeautifulSoup(page_source, "html.parser")

    select_anos = Select(driver.find_element(By.ID, "links-anos"))
    anos_meses = {}

    # Iterar sobre as opções dentro do select de anos
    for option_ano in select_anos.options:
        valor_ano = option_ano.get_attribute("value")
        ano = option_ano.text

        select_anos.select_by_value(valor_ano)

        driver.implicitly_wait(5)

        select_meses = Select(driver.find_element(By.ID, "links-meses"))

        meses_dict = {}

        # Iterar sobre as opções dentro do select de meses
        for option_mes in select_meses.options:
            valor_mes = option_mes.get_attribute("value")
            nome_mes = option_mes.text.capitalize()
            meses_dict[valor_mes] = nome_mes

        anos_meses[ano] = meses_dict

    driver.quit()

    anos = []
    meses_numeros = []
    meses_nomes = []

    # Iterar sobre o dicionário para extrair os dados
    for ano, meses in anos_meses.items():
        for mes_numero, mes_nome in meses.items():
            anos.append(ano)
            meses_numeros.append(mes_numero)
            meses_nomes.append(mes_nome)

    # Criar o DataFrame
    data = {"ano": anos, "mes_numero": meses_numeros, "mes_nome": meses_nomes}
    df = pd.DataFrame(data)
    df["urls"] = None
    for index, row in df.iterrows():
        if table == "Servidores_SIAPE":
            df["urls"][index] = f"{row.ano}{row.mes_numero}_Servidores_SIAPE.zip"

    return df
