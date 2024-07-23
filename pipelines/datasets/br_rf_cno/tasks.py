# -*- coding: utf-8 -*-
"""
Tasks for br_rf_cno
"""


import os
from bs4 import BeautifulSoup
from datetime import datetime
import requests
import pandas as pd
from tqdm import tqdm

from prefect import task

from pipelines.datasets.br_rf_cno.constants import constants as br_rf_cno
from pipelines.utils.utils import log



@task
def check_need_for_update(url:str)-> DateTime:

    response = requests.get(url)

    if response.status_code != 200:
        raise requests.HTTPError(f"HTTP error occurred: Status code {response.status_code}")

    Log('---- Checking need for update')

    soup = BeautifulSoup(response.content, 'html.parser')
    element = soup.select_one('table tr:nth-of-type(4) td:nth-of-type(3)')

    if element:
        date_text = element.get_text(strip=True)

        date_obj = datetime.strptime(date_text, "%Y-%m-%d %H:%M")
        formatted_date = date_obj.strftime("%Y-%m-%d")

        return formatted_date

    else:
        raise ValueError(F"The Last update data was not found in --- URL {url}. The website HTML code might have changed")


@task
def crawl_cno(root: str, url: str, chunk_size=1024) -> None:
    """Download and unzip tables from br_rf_cno"""
    filepath = f"{root}/data.zip"
    os.makedirs(root, exist_ok=True)
    os.makedirs(f"{root}/cleaned", exist_ok=True)

    log(f'----- Downloading files from {url}')

    response = requests.get(url, stream=True, timeout=300)
    total_size = int(response.headers.get('content-length', 0))

    with open(filepath, "wb") as file:
        for chunk in tqdm(response.iter_content(chunk_size=chunk_size), total=total_size // chunk_size, unit='KB', unit_scale=True):
            file.write(chunk)

    shutil.unpack_archive(filepath, extract_dir=root)



