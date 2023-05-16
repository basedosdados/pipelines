# -*- coding: utf-8 -*-
"""
General purpose functions for the br_bcb_estban project
"""
import requests
from lxml import html
import basedosdados as bd
from io import BytesIO
from zipfile import ZipFile
from urllib.request import urlopen
import pandas as pd
import numpy as np
import os
from pipelines.utils.utils import (
    log,
)

# ------- macro etapa 1 download de dados


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


# ------- macro etapa 2 tratamento de dados
# --- read files
def read_files(path: str) -> pd.DataFrame:
    """This function read a file from a given path

    Args:
        path (str): a path to a file

    Returns:
        pd.DataFrame: a dataframe with the file data
    """
    df = pd.read_csv(
        path,
        sep=";",
    )

    return df
