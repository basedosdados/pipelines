# -*- coding: utf-8 -*-
""" Utils for the Brazilian Comex Stat pipeline. """
# pylint: disable=invalid-name
import os
import wget
import time as tm
from tqdm import tqdm
from pipelines.utils.utils import (
    log,
)


def create_paths(
    path: str,
    table_name: str,
):
    """this function creates temporary directories to store input/output files

    Args:
        path (str): a standard directory to store input and output files from all flows
        table_name (str): the name of the table to compose the directory structure and separate input and output files
        of diferent tables

    """
    path_temps = [
        path,
        path + table_name + "/input/",
        path + table_name + "/output/",
    ]

    for path_temp in path_temps:
        os.makedirs(path_temp, exist_ok=True)


def download_data(
    path: str,
    table_type: str,
    table_name: str,
):
    """A simple crawler to download data from comex stat website.

    Args:
        path (str): the path to store the data
        table_type (str): the table type is either ncm or mun. ncm stands for 'nomenclatura comum do mercosul' and
        mun for 'município'.
        table_name (str): the table name is the original name of the zip file with raw data from comex stat website
    """
    # years = [2023]
    for year in range(2023, 2024):
        # append mode setted, so during 2023 year, the crawler will only
        # download 2023 file and uptade it

        table_name_urls = {
            "mun_imp": f"https://balanca.economia.gov.br/balanca/bd/comexstat-bd/{table_type}/IMP_{year}_MUN.csv",
            "mun_exp": f"https://balanca.economia.gov.br/balanca/bd/comexstat-bd/{table_type}/EXP_{year}_MUN.csv",
            "ncm_imp": f"https://balanca.economia.gov.br/balanca/bd/comexstat-bd/{table_type}/IMP_{year}.csv",
            "ncm_exp": f"https://balanca.economia.gov.br/balanca/bd/comexstat-bd/{table_type}/EXP_{year}.csv",
        }

        # selects a url given a table name
        url = table_name_urls[table_name]

        log(f"Downloading {url}")

        # downloads the file and saves it
        wget.download(url, out=path + table_name + "/input")
        # just for precaution,
        # sleep for 8 secs in between iterations
        tm.sleep(8)
