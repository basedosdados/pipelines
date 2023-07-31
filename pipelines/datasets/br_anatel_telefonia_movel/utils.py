# -*- coding: utf-8 -*-
"""
General purpose functions for the br_anatel_telefonia_movel project of the pipelines
"""
# pylint: disable=too-few-public-methods,invalid-name

from io import BytesIO
from zipfile import ZipFile
from urllib.request import urlopen
import os
import datetime


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
