# -*- coding: utf-8 -*-
"""
Tasks for br_rf_cno
"""
from datetime import datetime, timedelta
from pipelines.constants import constants
import os
from bs4 import BeautifulSoup
from datetime import datetime
import requests
import pandas as pd
import shutil
import asyncio

from prefect import task

from pipelines.datasets.br_rf_cno.constants import constants as br_rf_cno
from pipelines.datasets.br_rf_cno.utils import *
from pipelines.utils.utils import log


#NOTE: O crawler falhará se o nome do arquivo mudar.
@task
def check_need_for_update(url: str) -> str:
    """
    Checks the need for an update by extracting the most recent update date for 'cno.zip' from the CNO FTP.

    Args:
        url (str): The URL of the CNO FTP site.

    Returns:
        str: The date of the last update in 'YYYY-MM-DD' format.

    Raises:
        requests.HTTPError: If there is an HTTP error when making the request.
        ValueError: If the file 'cno.zip' is not found in the URL.
    """
    log('---- Extracting most recent update date from CNO FTP')

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "pt-BR,pt;q=0.8,en-US;q=0.5,en;q=0.3",
        "Sec-GPC": "1",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-User": "?1",
        "Priority": "u=0, i"
    }

    response = requests.get(url, headers=headers, timeout=(10, 30))

    if response.status_code != 200:
        raise requests.HTTPError(f"HTTP error occurred: Status code {response.status_code}")


    soup = BeautifulSoup(response.content, 'html.parser')
    rows = soup.find_all('tr')


    max_file_date = None

    # A lógica é simples: processa cada 'table data' (td) de cada linha 'tr'
    for row in rows:
        cells = row.find_all('td')


        if len(cells) < 4:
            continue


        link = cells[1].find('a')
        if not link:
            continue

        name = link.get_text(strip=True)
        if name != "cno.zip":
            continue


        date = cells[2].get_text(strip=True)
        max_file_date = datetime.strptime(date, "%Y-%m-%d %H:%M").strftime("%Y-%m-%d")
        break

    if not max_file_date:
        raise ValueError("File 'cno.zip' not found on the FTP site. Check the api endpoint: https://arquivos.receitafederal.gov.br/dados/cno/ to see folder structure or file name has changed")

    log(f"---- Most recent update date for 'cno.zip': {max_file_date}")

    return max_file_date









@task
def wrangling(input_dir: str, output_dir: str, partition_date: str) -> None:
    """
    Processes and converts CSV files to Parquet format, renaming tables and columns as specified.

    Args:
        input_dir (str): The directory where the input CSV files are located.
        output_dir (str): The directory where the output Parquet files will be saved.
        partition_date (str): The partition date to be used in the output directory structure.

    Returns:
        None
    """
    table_rename = br_rf_cno.TABLES_RENAME.value
    columns_rename = br_rf_cno.COLUMNS_RENAME.value

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

            df = df.applymap(str)

            parquet_file = v + '.parquet'
            partition_folder = f'data={partition_date}'
            output_folder = os.path.join(output_dir, v, partition_folder)

            os.makedirs(output_folder, exist_ok=True)

            parquet_path = os.path.join(output_folder, parquet_file)

            df.to_parquet(parquet_path, index=False)

            os.remove(file_path)

    log('----- Wrangling completed')


@task(
    max_retries=5,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def crawl_cno(root: str, url: str) -> None:
    """
    Downloads and unpacks a ZIP file from the given URL.

    Args:
        root (str): The root directory where the file will be saved and unpacked.
        url (str): The URL of the ZIP file to be downloaded.

    Returns:
        None
    """
    asyncio.run(download_file_async(root, url))

    filepath = f"{root}/data.zip"
    print(f'----- Unzipping files from {filepath}')
    shutil.unpack_archive(filepath, extract_dir=root)
    os.remove(filepath)
    print('----- Download and unpack completed')


@task
def create_parameters_list(
    dataset_id: str,
    table_ids: list,
    materialization_mode: str,
    dbt_alias: str,
    dbt_command: str,
    disable_elementary: bool,
    download_csv_file: bool
) -> list:
    """
    Generates a list of parameters for the DBT materialization flow.

    Args:
        dataset_id (str): The dataset ID.
        table_ids (list): A list of table IDs.
        materialization_mode (str): The materialization mode.
        dbt_alias (str): The DBT alias.
        dbt_command (str): The DBT command.
        disable_elementary (bool): Whether to disable elementary.
        download_csv_file (bool): Whether to download the CSV file.

    Returns:
        list: A list of dictionaries containing the parameters for each table.
    """
    log('----- Generating DBT parameters for Materialization Flow')
    return [
        {
            "dataset_id": dataset_id,
            "table_id": table_id,
            "mode": materialization_mode,
            "dbt_alias": dbt_alias,
            "dbt_command": dbt_command,
            "disable_elementary": disable_elementary,
            "download_csv_file": download_csv_file
        }
        for table_id in table_ids
    ]
