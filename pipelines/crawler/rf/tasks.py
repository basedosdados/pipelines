"""
Generic Tasks for br_rf
"""

import asyncio
import os
import shutil
import time
from datetime import date, datetime
from pathlib import Path

import httpx
import pandas as pd
from prefect import task
from tqdm import tqdm

from pipelines.constants import constants
from pipelines.crawler.rf.constants import constants as br_rf_constants
from pipelines.crawler.rf.utils import (
    download_file_async,
    process_chunk,
)
from pipelines.utils.utils import log


@task
def check_need_for_update(dataset_id: str, url: str | None = None) -> date:
    """
    Checks the need for an update by reading the source file's ``Last-Modified``
    HTTP header (via a HEAD request).

    A fonte (WebDAV ownCloud da Receita Federal) passou a ficar atrás de um WAF
    (F5 BIG-IP) que rejeita o método PROPFIND com ``HTTP 200`` + página HTML
    "Request Rejected". Por isso a data de referência agora é lida do header
    ``Last-Modified`` do próprio ``cno.zip`` (cujo GET/HEAD não é bloqueado),
    em vez da listagem via PROPFIND.

    Args:
        dataset_id (str): RF specific dataset name.

    Returns:
        datetime.date: The source file's last-modified date.

    Raises:
        httpx.HTTPError: If the request keeps failing after exhausting retries.
        ValueError: If the ``Last-Modified`` header is missing/unparseable.

    Notes:
        - Implementa retries com backoff exponencial para falhas transitórias
          (conexão e status 5xx).
    """
    log(f"---- Checking most recent update date for {dataset_id}")
    retries = 5
    delay = 2

    if url is None:
        url = br_rf_constants.URLS.value["url_download"]

    for attempt in range(retries):
        try:
            response = httpx.head(
                url,
                headers=br_rf_constants.HEADERS.value,
                timeout=30,
                follow_redirects=True,
            )
            response.raise_for_status()
            break
        except httpx.HTTPError as e:
            log(f"Request attempt {attempt + 1}/{retries} failed: {e}")
            if attempt < retries - 1:
                time.sleep(delay)
                delay *= 2
            else:
                raise

    most_recent_date = response.headers.get("Last-Modified")

    if not most_recent_date:
        raise ValueError(
            f"Could not read Last-Modified from {url}. "
            "Check if the source or its response headers have changed."
        )

    parsed_most_recent_date = datetime.strptime(
        most_recent_date, "%a, %d %b %Y %H:%M:%S GMT"
    ).date()

    log(f"Most recent update date: {parsed_most_recent_date}")
    return parsed_most_recent_date


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
        partition_date = datetime.strptime(str(partition_date), "%Y-%m-%d")
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
    retries=2,
    retry_delay_seconds=constants.TASK_RETRY_DELAY.value,
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
        url = br_rf_constants.URLS.value["url_download"]
    log(f"---- Downloading CNO file from {url}")
    os.makedirs(dataset_id, exist_ok=True)

    asyncio.run(download_file_async(f"{dataset_id}/{input_dir}", f"{url}"))

    filepath = f"{dataset_id}/{input_dir}/data.zip"
    log(f"---- Unzipping files from {filepath}")
    shutil.unpack_archive(filepath, extract_dir=f"{dataset_id}/{input_dir}")
    os.remove(filepath)
    log("Download and unpack completed successfully")
