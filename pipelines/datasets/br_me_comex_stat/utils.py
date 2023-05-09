# -*- coding: utf-8 -*-
""" Utils for the Brazilian Comex Stat pipeline. """
# pylint: disable=invalid-name
import os
import requests

from tqdm import tqdm
from pipelines.utils.utils import (
    log,
)


def create_paths(
    path: str,
    table_name: str,
):
    """this function creates temporary directories to store input and output files

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

    log(f"Downloading {table_type} of {table_name}")
    url = f"https://balanca.economia.gov.br/balanca/bd/comexstat-bd/{table_type}/{table_name}.zip"

    r = requests.get(url, verify=False, timeout=99999999)
    with open(path + f"{table_name}/input/{table_name}.zip", "wb") as f:
        f.write(r.content)
