# -*- coding: utf-8 -*-

import asyncio
import os
from typing import List

import httpx
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm

from pipelines.datasets.br_rf_cno.constants import constants as br_rf_cno
from pipelines.utils.utils import log


async def download_chunk(
    client: httpx.AsyncClient,
    url: str,
    start: int,
    end: int,
    filepath: str,
    semaphore: asyncio.Semaphore,
    pbar: tqdm,
) -> None:
    """
    Downloads a specific byte range (chunk) of a file from the given URL and writes it
    to the correct position in the target file.

    Args:
        client (httpx.AsyncClient): The HTTP client used for making requests.
        url (str): The URL of the file to be downloaded.
        start (int): The starting byte position of the chunk.
        end (int): The ending byte position of the chunk.
        filepath (str): The file path where the chunk will be written.
        semaphore (asyncio.Semaphore): Semaphore to limit concurrent downloads.
        pbar (tqdm.tqdm): Progress bar to update download status.

    Returns:
        None
    """
    headers = {
        "Range": f"bytes={start}-{end}",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/80.0.3987.149 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "pt-BR,pt;q=0.8,en-US;q=0.5,en;q=0.3",
        "Sec-GPC": "1",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-User": "?1",
        "Priority": "u=0, i",
    }

    async with semaphore:  # controla concorrência
        response = await client.get(url, headers=headers, timeout=60.0)
        with open(filepath, "r+b") as f:
            f.seek(start)  # posiciona no ponto certo do arquivo
            f.write(response.content)
            pbar.update(len(response.content))


async def download_file_async(root: str, url: str) -> None:
    """
    Downloads a file asynchronously in chunks and saves it into the given directory.

    Workflow:
        1. Send HEAD request to determine total file size.
        2. Pre-allocate the file with zeros.
        3. Split download into chunks (16 MB each).
        4. Download chunks concurrently (max 5 at a time).
        5. Write each chunk into the correct file offset.
        6. Track progress with tqdm.

    Args:
        root (str): Root directory where the file will be saved.
        url (str): The URL of the file to download.

    Returns:
        None
    """
    filepath = f"{root}/data.zip"
    os.makedirs(root, exist_ok=True)

    log(f"---- Starting async download from {url}")

    # Step 1: get file size
    async with httpx.AsyncClient() as client:
        response = await client.head(url, timeout=60.0)
        total_size = int(response.headers["content-length"])

    # Step 2: preallocate space
    with open(filepath, "wb") as f:
        f.write(b"\0" * total_size)

    chunk_size = 1024 * 1024 * 16  # 16 MB per chunk
    tasks: List[asyncio.Task] = []
    semaphore = asyncio.Semaphore(5)  # allow max 5 simultaneous downloads

    # Step 3: schedule downloads
    with tqdm(
        total=total_size, unit="MB", unit_scale=True, desc=filepath
    ) as pbar:
        async with httpx.AsyncClient() as client:
            for start in range(0, total_size, chunk_size):
                end = min(start + chunk_size - 1, total_size - 1)
                tasks.append(
                    download_chunk(
                        client, url, start, end, filepath, semaphore, pbar
                    )
                )
            await asyncio.gather(*tasks)

    log(f"Download completed: {filepath} ({total_size / (1024**2):.2f} MB)")


def process_chunk(
    df_chunk: pd.DataFrame,
    chunk_index: int,
    output_dir: str,
    partition_date: str,
    table_name: str,
) -> None:
    """
    Processes a DataFrame chunk by renaming columns, converting to Parquet format,
    and saving into a partitioned folder structure.

    Args:
        df_chunk (pd.DataFrame): DataFrame chunk to process.
        chunk_index (int): Sequential index of the chunk (used in filename).
        output_dir (str): Directory to save the output Parquet files.
        partition_date (str): Partition value (YYYY-MM-DD).
        table_name (str): Name of the table (used in folder/filename).

    Returns:
        None
    """

    columns_rename = br_rf_cno.COLUMNS_RENAME.value
    if table_name in columns_rename:
        df_chunk = df_chunk.rename(columns=columns_rename[table_name])

    # Ensure all values are strings to avoid schema issues
    df_chunk = df_chunk.applymap(str)

    parquet_file = f"{table_name}_{chunk_index}.parquet"
    partition_folder = f"data={partition_date}"
    output_folder = os.path.join(output_dir, table_name, partition_folder)
    parquet_path = os.path.join(output_folder, parquet_file)

    os.makedirs(output_folder, exist_ok=True)

    # Convert DataFrame → Arrow Table → Parquet
    table = pa.Table.from_pandas(df_chunk)
    pq.write_table(table, parquet_path)

    log(f"   ↳ Saved chunk {chunk_index} to {parquet_path}")
    del df_chunk, table  # libera memória
