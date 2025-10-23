"""
Generic Tasks for br_rf
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
from tqdm import tqdm

from pipelines.constants import constants
from pipelines.crawler.rf.constants import constants as br_rf_constants
from pipelines.crawler.rf.utils import (
    download_file_async,
    process_chunk,
)
from pipelines.utils.utils import log


@task
def check_need_for_update(dataset_id: str, url: str | None = None) -> str:
    """
    Checks the need for an update by extracting the most recent update date
    for  source files from the url listing.

    Args:
        dataset_id (str): RF specific dataset name.

    Returns:
        str: The date of the last update in 'YYYY-MM-DD' format.

    Raises:
        requests.HTTPError: If there is an HTTP error when making the request.
        ValueError: If the file 'cno.zip' is not found in the URL.

    Notes:
        - O crawler falhará se o nome do arquivo mudar.
        - Implementa retries com backoff exponencial para falhas de conexão.
    """
    log(f"---- Checking most recent update date for {dataset_id}")
    retries = 5
    delay = 2

    if url is None:
        url = br_rf_constants.URLS.value[dataset_id]
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
            raise requests.HTTPError(f"HTTP error occurred: {e}") from e

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
        if str(name).split(".")[0] not in dataset_id:
            continue

        date = cells[2].get_text(strip=True)
        max_file_date = datetime.strptime(date, "%Y-%m-%d %H:%M").strftime(
            "%Y-%m-%d"
        )
        break

    if not max_file_date:
        raise ValueError(
            f"File not found on {url}."
            "Check if the folder structure or file name has changed."
        )

    log(f"Most recent update date: {max_file_date}")
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
def process_file(
    dataset_id: str,
    table_id: str,
    input_dir: str,
    output_dir: str,
    partition_date: str,
    file: str | None = None,
    chunksize: int = 1000000,
) -> str | None:
    """
    Processes and converts a CSV file to Parquet format in chunks.
    Applies renaming of tables and columns as specified.

    Args:
        dataset_id (str): RF specific dataset name.
        input_dir (str): Directory containing input CSVs.
        output_dir (str): Directory where Parquet files will be saved.
        partition_date (str): Partition date (YYYY-MM-DD).
        file (str): Filename (must exist in input_dir).
        chunksize (int): Number of rows per chunk when reading CSV.

    Returns:
        None
    """

    if file is None:
        file = br_rf_constants.TABLES_RENAME.value[dataset_id][table_id]
    try:
        partition_date = datetime.strptime(
            partition_date, "%Y-%m-%d"
        ).strftime("%Y-%m-%d")
        log(f"Partition date {partition_date}.")
    except ValueError:
        log("Invalid partition_date format. Using raw value.")

    table_rename = br_rf_constants.TABLES_RENAME.value[dataset_id]
    filename = Path(file).name
    log(f"Processing file: {file}")

    if filename.endswith(".csv") and filename in table_rename.values():
        filepath = os.path.join(dataset_id, input_dir, filename)
        output_path = os.path.join(dataset_id, output_dir, table_id)
        os.makedirs(output_path, exist_ok=True)

        try:
            log(f"---- Processing file {filename} as table {table_id}")

            # N Chunks
            with open(filepath, encoding="latin-1") as fp:
                total_rows = sum(1 for _ in fp) - 1  # -1 header
                total_chunks = (total_rows // chunksize) + 1

            with tqdm(
                total=total_chunks, desc=f"{table_id}", unit="chunk"
            ) as pbar:
                for i, chunk in enumerate(
                    pd.read_csv(
                        filepath,
                        dtype=str,
                        encoding="latin-1",
                        sep=",",
                        chunksize=chunksize,
                    )
                ):
                    process_chunk(
                        dataset_id,
                        chunk,
                        i,
                        output_dir,
                        partition_date,
                        table_id,
                    )
                    pbar.update(1)

            os.remove(filepath)
            log(f"Removed temporary CSV {filename}")
            return output_path
        except Exception as e:
            log(f"Unable to process file: {e}", "error")
    else:
        log(f"File {file} not recognized in TABLES_RENAME, skipped.")


@task(
    max_retries=2,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def crawl(dataset_id: str, input_dir: str, url: str | None = None) -> None:
    """
    Downloads and unpacks the 'cno.zip' file from the given URL.

    Args:
        dataset_id (str): RF specific dataset name.
        input_dir (str): Input directory to save and unpack the file.
        url (str): URL of the ZIP file.

    Returns:
        None
    """
    if url is None:
        url = br_rf_constants.URLS.value[dataset_id]
    log(f"---- Downloading CNO file from {url}")
    os.makedirs(dataset_id, exist_ok=True)

    filename = "cno.zip"
    asyncio.run(
        download_file_async(f"{dataset_id}/{input_dir}", f"{url}{filename}")
    )

    filepath = f"{dataset_id}/{input_dir}/data.zip"
    log(f"---- Unzipping files from {filepath}")
    shutil.unpack_archive(filepath, extract_dir=f"{dataset_id}/{input_dir}")
    os.remove(filepath)
    log("Download and unpack completed successfully")
