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

import numpy as np
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from pipelines.utils.utils import log


def download_and_unzip(url, path):
    """download and unzip a zip file

    Args:
        url (str): a url


    Returns:
        list: unziped files in a given folder
    """

    os.system(f"mkdir -p {path}")

    http_response = urlopen(url)
    zipfile = ZipFile(BytesIO(http_response.read()))
    zipfile.extractall(path=path)

    return path


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


def data_url():
    # Configurar as opções do ChromeDriver
    options = webdriver.ChromeOptions()

    # Adicionar argumentos para executar o Chrome em modo headless (sem interface gráfica)
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--headless=new")

    # Inicializar o driver do Chrome com as opções configuradas
    driver = webdriver.Chrome(options=options)

    # URL da página da web que você deseja acessar
    url = "https://informacoes.anatel.gov.br/paineis/acessos/telefonia-movel"

    element_html = None
    
    try:
        # Abra a página da web
        driver.get(url)

        # Aguarde até que o elemento desejado seja carregado (você pode ajustar o tempo limite conforme necessário)
        element = driver.find_element(
            By.XPATH, "//div[@ng-class='{locked:item.qLocked}']"
        )

        # Obtenha o HTML do elemento
        element_html = element.get_attribute("outerHTML")

        # Imprima o HTML do elemento
        log(element_html)
    except Exception as e:
        log("Ocorreu um erro ao acessar a página:", str(e))
    finally:
        # Certifique-se de fechar o navegador, mesmo em caso de erro
        driver.quit()

    return element_html


def setting_data_url():
    meses = {
        "jan": "01",
        "fev": "02",
        "mar": "03",
        "abr": "04",
        "mai": "05",
        "jun": "06",
        "jul": "07",
        "ago": "08",
        "set": "09",
        "out": "10",
        "nov": "11",
        "dez": "12",
    }
    string_element = data_url()
    elemento_total = string_element[177:185]
    mes, ano = elemento_total.split("-")
    mes = meses[mes]
    data_total = f"{ano}-{mes}"
    log(data_total)

    return data_total
