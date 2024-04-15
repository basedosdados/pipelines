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
from pipelines.utils.utils import log, to_partitions
import numpy as np
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

def treatment(table_id:str, ano: int):
    log("Iniciando o tratamento do arquivo microdados da Anatel")
    df = pd.read_csv(
        f"{anatel_constants.INPUT_PATH.value}Acessos_Banda_Larga_Fixa_{ano}.csv",
        sep=";",
        encoding="utf-8",
    )
    df = check_and_create_column(df, "Tipo de Produto")
    df.rename(
        columns=anatel_constants.RENAME_MICRODADOS.value,
        inplace=True,
    )
    df.drop(anatel_constants.DROP_COLUMNS_MICRODADOS.value, axis=1, inplace=True)
    df = df[
        anatel_constants.ORDER_COLUMNS_MICRODADOS.value
    ]
    df.sort_values(
        anatel_constants.SORT_VALUES_MICRODADOS.value,
        inplace=True,
    )
    df.replace(np.nan, "", inplace=True)
    df["transmissao"] = df["transmissao"].apply(
        lambda x: x.replace("Cabo Metálico", "Cabo Metalico")
        .replace("Satélite", "Satelite")
        .replace("Híbrido", "Hibrido")
        .replace("Fibra Óptica", "Fibra Optica")
        .replace("Rádio", "Radio")
    )
    df["acessos"] = df["acessos"].apply(lambda x: str(x).replace(".0", ""))
    df["produto"] = df["produto"].apply(
        lambda x: x.replace("LINHA_DEDICADA", "linha dedicada").lower()
    )
    log("Salvando o arquivo microdados da Anatel")
    to_partitions(
        df,
        partition_columns=["ano", "mes", "sigla_uf"],
        savepath=anatel_constants.TABLES_OUTPUT_PATH.value[table_id],
    )


def treatment_br(table_id:str):
    log("Iniciando o tratamento do arquivo densidade brasil da Anatel")
    df = pd.read_csv(
        f"{anatel_constants.INPUT_PATH.value}Densidade_Banda_Larga_Fixa.csv",
        sep=";",
        encoding="utf-8",
    )
    df.rename(columns={"Nível Geográfico Densidade": "Geografia"}, inplace=True)
    df_brasil = df[df["Geografia"] == "Brasil"]
    df_brasil = df_brasil.drop(anatel_constants.DROP_COLUMNS_BRASIL.value, axis=1)
    df_brasil["Densidade"] = df_brasil["Densidade"].apply(
        lambda x: float(x.replace(",", "."))
    )
    df_brasil.rename(
        columns=anatel_constants.RENAME_COLUMNS_BRASIL.value,
        inplace=True,
    )
    log('Salvando o arquivo densidade brasil da Anatel')
    df_brasil.to_csv(
        f"{anatel_constants.TABLES_OUTPUT_PATH.value[table_id]}densidade_brasil.csv",
        index=False,
        sep=",",
        encoding="utf-8",
        na_rep="",
    )


def treatment_uf(table_id:str):

    log("Iniciando o tratamento do arquivo densidade uf da Anatel")
    df = pd.read_csv(
        f"{anatel_constants.INPUT_PATH.value}Densidade_Banda_Larga_Fixa.csv",
        sep=";",
        encoding="utf-8",
    )
    df.rename(columns={"Nível Geográfico Densidade": "Geografia"}, inplace=True)
    df_uf = df[df["Geografia"] == "UF"]
    df_uf.drop(anatel_constants.DROP_COLUMNS_UF.value, axis=1, inplace=True)
    df_uf["Densidade"] = df_uf["Densidade"].apply(lambda x: float(x.replace(",", ".")))
    df_uf.rename(
        columns=anatel_constants.RENAME_COLUMNS_UF.value,
        inplace=True,
    )
    log("Iniciando o particionado do arquivo densidade uf da Anatel")
    df_uf.to_csv(
        f"{anatel_constants.TABLES_OUTPUT_PATH.value[table_id]}densidade_uf.csv",
        index=False,
        sep=",",
        encoding="utf-8",
        na_rep="",
    )



def treatment_municipio(table_id:str):
    log("Iniciando o tratamento do arquivo densidade municipio da Anatel")
    df = pd.read_csv(
        f"{anatel_constants.INPUT_PATH.value}Densidade_Banda_Larga_Fixa.csv",
        sep=";",
        encoding="utf-8",
    )
    df.rename(columns={"Nível Geográfico Densidade": "Geografia"}, inplace=True)
    df_municipio = df[df["Geografia"] == "Municipio"]
    df_municipio.drop(["Município", "Geografia"], axis=1, inplace=True)
    df_municipio["Densidade"] = df_municipio["Densidade"].apply(
        lambda x: float(x.replace(",", "."))
    )
    df_municipio.rename(
        columns=anatel_constants.RENAME_COLUMNS_MUNICIPIO.value,
        inplace=True,
    )
    log("Salvando o arquivo densidade municipio da Anatel")
    to_partitions(
        df_municipio,
        partition_columns=["ano"],
        savepath=anatel_constants.TABLES_OUTPUT_PATH.value[table_id],
    )