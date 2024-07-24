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
import shutil

from prefect import task

from pipelines.datasets.br_rf_cno.constants import constants as br_rf_cno
from pipelines.utils.utils import log



@task
def check_need_for_update(url:str)-> None:

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
def crawl_cno(root: str, url: str) -> None:
    """Download and unzip tables from br_rf_cno"""
    filepath = f"{root}/data.zip"
    os.makedirs(root, exist_ok=True)

    print(f'----- Downloading files from {url}')

    response = requests.get(url, stream=True, timeout=300)
    total_size = int(response.headers.get('content-length', 0))

    with open(filepath, "wb") as file:
        for data in tqdm(response.iter_content(1024), total=total_size // 1024, unit='KB'):
            file.write(data)

    shutil.unpack_archive(filepath, extract_dir=root)
    os.remove(filepath)  # Remove the zip file after unpacking

    print('----- Download and unpack completed')


@task
def wrangling(root: str):
    paths = os.listdir(root)

    for file in paths:
        if file.endswith('.csv'):

            file_path = os.path.join(root, file)

            df = pd.read_csv(file_path, dtype=str, encoding='latin-1', sep=',', na_rep='')

            parquet_file = file.replace('.csv', '.parquet')
            parquet_path = os.path.join(root, parquet_file)

            df.to_parquet(parquet_path, index=False)

            os.remove(file_path)