# -*- coding: utf-8 -*-
"""
Tasks for br_rf_cno
"""

import asyncio
import os
import shutil
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup
from prefect import task
from requests.exceptions import ConnectionError, HTTPError

from pipelines.constants import constants
from pipelines.datasets.br_rf_cno.constants import constants as br_rf_cno
from pipelines.datasets.br_rf_cno.utils import *  # noqa: F403
from pipelines.utils.utils import log


@task
def check_need_for_update(url: str) -> str:
    """
    Checks the need for an update by extracting the most recent update date
    for 'cno.zip' from the CNO FTP directory listing.

    Args:
        url (str): The URL of the CNO FTP site.

    Returns:
        str: The date of the last update in 'YYYY-MM-DD' format.

    Raises:
        requests.HTTPError: If there is an HTTP error when making the request.
        ValueError: If the file 'cno.zip' is not found in the URL.

    Notes:
        - O crawler falhará se o nome do arquivo mudar.
        - Implementa retries com backoff exponencial para falhas de conexão.
    """
    log("---- Checking most recent update date for 'cno.zip' in CNO FTP")
    retries = 5
    delay = 2

    for attempt in range(retries):
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            break
        except ConnectionError as e:
            log(f"Connection attempt {attempt + 1}/{retries} failed: {e}")
            if attempt < retries - 1:
                time.sleep(delay)
                delay *= 2
            else:
                raise
        except HTTPError as e:
            raise requests.HTTPError(f"HTTP error occurred: {e}")

    soup = BeautifulSoup(response.content, "html.parser")
    rows = soup.find_all("tr")

    max_file_date = None

    # Percorre linhas da tabela do FTP procurando o arquivo alvo
    for row in rows:
        cells = row.find_all("td")

        if len(cells) < 4:  # Espera estrutura <td> com link e data
            continue

        link = cells[1].find("a")
        if not link:
            continue

        name = link.get_text(strip=True)
        if name != "cno.zip":
            continue

        date = cells[2].get_text(strip=True)
        max_file_date = datetime.strptime(date, "%Y-%m-%d %H:%M").strftime(
            "%Y-%m-%d"
        )
        break

    if not max_file_date:
        raise ValueError(
            "File 'cno.zip' not found on the FTP site. "
            "Check if the folder structure or file name has changed."
        )

    log(f"Most recent update date for 'cno.zip': {max_file_date}")
    return max_file_date


@task
def list_files(input_dir: str) -> list:
    """
    Lists all CSV files present in the input directory.

    Args:
        input_dir (str): Directory where input CSVs are stored.

    Returns:
        list: List of filenames (only names, no paths).
    """
    log(f"---- Listing CSV files in {input_dir}")
    paths = [fp.name for fp in Path(input_dir).glob("*.csv")]
    paths_string = "\n".join(paths)
    log(f"Found {len(paths)} CSV files:\n{paths_string}")
    return paths


@task
def process_file(file, input_dir, output_dir, partition_date, chunksize):
    """
    Processes and converts a CSV file to Parquet format in chunks.
    Applies renaming of tables and columns as specified.

    Args:
        file (str): Filename (must exist in input_dir).
        input_dir (str): Directory containing input CSVs.
        output_dir (str): Directory where Parquet files will be saved.
        partition_date (str): Partition date (YYYY-MM-DD).
        chunksize (int): Number of rows per chunk when reading CSV.

    Returns:
        None
    """
    try:
        partition_date = datetime.strptime(
            partition_date, "%Y-%m-%d"
        ).strftime("%Y-%m-%d")
        log(f"Partition date {partition_date}.")
    except ValueError:
        log("Invalid partition_date format. Using raw value.")
    breakpoint()
    table_rename = br_rf_cno.TABLES_RENAME.value
    log(f"Processing file:{file}")
    filename = Path(file).name
    breakpoint()
    if filename.endswith(".csv") and file in table_rename:
        breakpoint()
        filepath = os.path.join(input_dir, filename)
        table_name = table_rename[filename]

        log(f"---- Processing file {filename} as table {table_name}")
        for i, chunk in enumerate(
            pd.read_csv(
                filepath,
                dtype=str,
                encoding="latin-1",
                sep=",",
                chunksize=chunksize,
            )
        ):
            log(f"   ↳ Processing chunk {i + 1}")
            process_chunk(chunk, i, output_dir, partition_date, table_name)  # noqa: F405

        os.remove(filepath)  # Remove CSV após processamento
        log(f"Removed temporary CSV {filename}")
    else:
        log(f"File {file} not recognized in TABLES_RENAME, skipped.")


@task(
    max_retries=2,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def crawl_cno(root: str, url: str) -> None:
    """
    Downloads and unpacks the 'cno.zip' file from the given URL.

    Args:
        root (str): Root directory to save and unpack the file.
        url (str): URL of the ZIP file.

    Returns:
        None
    """
    log(f"---- Downloading CNO file from {url}")
    asyncio.run(download_file_async(root, url))  # noqa: F405

    filepath = f"{root}/data.zip"
    log(f"---- Unzipping files from {filepath}")
    shutil.unpack_archive(filepath, extract_dir=root)
    os.remove(filepath)
    log("Download and unpack completed successfully")


@task
def create_parameters_list(
    dataset_id: str,
    table_ids: list,
    target: str,
    dbt_alias: str,
    dbt_command: str,
    disable_elementary: bool,
    download_csv_file: bool,
) -> list:
    """
    Generates a list of parameters for the DBT materialization flow.

    Args:
        dataset_id (str): Dataset ID in BigQuery.
        table_ids (list): List of table IDs to materialize.
        target (str): Materialization target (e.g., prod, dev).
        dbt_alias (str): DBT alias for execution.
        dbt_command (str): DBT command to run (e.g., run, build).
        disable_elementary (bool): Whether to disable elementary checks.
        download_csv_file (bool): Whether to include CSV download flag.

    Returns:
        list: A list of parameter dicts, one per table.
    """
    log("---- Generating DBT parameters for Materialization Flow")
    params = [
        {
            "dataset_id": dataset_id,
            "table_id": table_id,
            "target": target,
            "dbt_alias": dbt_alias,
            "dbt_command": dbt_command,
            "disable_elementary": disable_elementary,
            "download_csv_file": download_csv_file,
        }
        for table_id in table_ids
    ]
    log(f"Generated parameters for {len(table_ids)} tables")
    return params
