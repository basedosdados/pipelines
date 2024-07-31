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
import subprocess
import aiohttp
import asyncio
import httpx

from prefect import task

from pipelines.datasets.br_rf_cno.constants import constants as br_rf_cno
from pipelines.utils.utils import log



@task
def check_need_for_update(url:str)-> None:
    log('---- Extracting most recent update date from CNO FTP')
    response = requests.get(url)

    if response.status_code != 200:
        raise requests.HTTPError(f"HTTP error occurred: Status code {response.status_code}")

    soup = BeautifulSoup(response.content, 'html.parser')
    element = soup.select_one('table tr:nth-of-type(4) td:nth-of-type(3)')

    if element:
        date_text = element.get_text(strip=True)

        date_obj = datetime.strptime(date_text, "%Y-%m-%d %H:%M")
        formatted_date = date_obj.strftime("%Y-%m-%d")
        log(f'---- The last update in original source occured in: {formatted_date}')
        return formatted_date

    else:
        raise ValueError(F"The Last update data was not found in --- URL {url}. The website HTML code might have changed")

#tentei com requests;
#tentei com urlib
#pycurl
#usando subprocess para usar o wget do linux
#os headers da resposta mostram que aceita downloads em bytes paralelizados;



@task
def crawl_cno(root: str, url: str) -> None:
    """Download and unzip tables from br_rf_cno"""
    filepath = f"{root}/data.zip"
    os.makedirs(root, exist_ok=True)

    log(f'----- Downloading files from {url}')

    response = requests.get(url, timeout=300, stream=True)
    total_size = int(response.headers.get('content-length', 0))

    with open(filepath, "wb") as file:
        with tqdm(total=total_size, unit='B', unit_scale=True, desc=filepath) as pbar:
            for chunk in response.iter_content(chunk_size=1024*1024):
                if chunk:
                    file.write(chunk)
                    pbar.update(len(chunk))

    log(f'----- Unzipping files from {filepath}')
    shutil.unpack_archive(filepath, extract_dir=root)
    os.remove(filepath)

    log('----- Download and unpack completed')


@task
def wrangling(input_dir: str, output_dir: str, partition_date: str):
    table_rename = br_rf_cno.TABLES_RENAME.value
    columns_rename = br_rf_cno.COLUMNS_RENAME.value

    # Validate the partition_date format
    try:
        partition_date = datetime.strptime(partition_date, '%Y-%m-%d')
        partition_date = partition_date.strftime('%Y-%m-%d')
    except ValueError:
        log('Invalid partition_date format.')

    paths = os.listdir(input_dir)

    for file in paths:
        if file.endswith('.csv') and file in table_rename:
            k = file
            v = table_rename[file]
            log(f'----- Processing file {k}')
            file_path = os.path.join(input_dir, file)

            df = pd.read_csv(file_path, dtype=str, encoding='latin-1', sep=',')

            if v in columns_rename:
                df = df.rename(columns=columns_rename[v])

            parquet_file = v + '.parquet'
            partition_folder = f'data={partition_date}'
            output_folder = os.path.join(output_dir, v, partition_folder)

            os.makedirs(output_folder, exist_ok=True)

            parquet_path = os.path.join(output_folder, parquet_file)

            df.to_parquet(parquet_path, index=False)

            os.remove(file_path)

    log('----- Wrangling completed')



async def download_chunk(client, url, start, end, filepath, semaphore, pbar):
    headers = {"Range": f"bytes={start}-{end}"}
    async with semaphore:
        response = await client.get(url, headers=headers, timeout=60.0)
        with open(filepath, "r+b") as f:
            f.seek(start)
            f.write(response.content)
            pbar.update(len(response.content))

async def download_file_async(root: str, url: str):
    filepath = f"{root}/data.zip"
    os.makedirs(root, exist_ok=True)

    print(f'----- Downloading files from {url}')

    async with httpx.AsyncClient() as client:
        response = await client.head(url, timeout=60.0)
        total_size = int(response.headers["content-length"])

    # Create a file with the total size
    with open(filepath, "wb") as f:
        f.write(b"\0" * total_size)

    chunk_size = 1024 * 1024 * 16  # 8 MB chunks
    tasks = []
    semaphore = asyncio.Semaphore(5)  # Limit to 10 concurrent downloads

    with tqdm(total=total_size, unit='MB', unit_scale=True, desc=filepath) as pbar:
        async with httpx.AsyncClient() as client:
            for start in range(0, total_size, chunk_size):
                end = min(start + chunk_size - 1, total_size - 1)
                tasks.append(download_chunk(client, url, start, end, filepath, semaphore, pbar))
            await asyncio.gather(*tasks)

    print(f'----- Downloading completed')

@task
def crawl_cno_2(root: str, url: str) -> None:
    asyncio.run(download_file_async(root, url))

    filepath = f"{root}/data.zip"
    print(f'----- Unzipping files from {filepath}')
    shutil.unpack_archive(filepath, extract_dir=root)
    os.remove(filepath)
    print('----- Download and unpack completed')



@task
def create_parameters_list(dataset_id, table_ids, materialization_mode, dbt_alias):
    return [
        {"dataset_id": dataset_id, "table_id": table_id, "mode": materialization_mode, "dbt_alias": dbt_alias}
        for table_id in table_ids
    ]