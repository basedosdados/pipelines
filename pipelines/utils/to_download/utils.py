# -*- coding: utf-8 -*-
import os
import zipfile
from asyncio import Semaphore, gather
from typing import List

import httpx

from pipelines.utils.utils import log


def chunk_range(content_length: int, chunk_size: int) -> list[tuple[int, int]]:
    """Split the content length into a list of chunk ranges"""
    return [
        (i * chunk_size, min((i + 1) * chunk_size - 1, content_length - 1))
        for i in range(content_length // chunk_size + 1)
    ]


async def download(
    url: str,
    chunk_size: int = 2**20,
    max_retries: int = 32,
    max_parallel: int = 16,
    timeout: int = 3 * 60 * 1000,
) -> bytes:
    request_head = httpx.head(url)

    assert request_head.status_code == 200
    assert request_head.headers["accept-ranges"] == "bytes"

    content_length = int(request_head.headers["content-length"])

    log(
        f"Downloading {url} with {content_length} bytes / {chunk_size} chunks and {max_parallel} parallel downloads"
    )

    # TODO: pool http connections
    semaphore = Semaphore(max_parallel)
    tasks = [
        download_chunk(url, (start, end), max_retries, timeout, semaphore)
        for start, end in chunk_range(content_length, chunk_size)
    ]

    return b"".join(await gather(*tasks))


async def download_chunk(
    url: str,
    chunk_range: tuple[int, int],
    max_retries: int,
    timeout: int,
    semaphore: Semaphore,
) -> bytes:
    async with semaphore:
        # log(f"Downloading chunk {chunk_range[0]}-{chunk_range[1]}")
        for i in range(max_retries):
            try:
                async with httpx.AsyncClient(timeout=timeout) as client:
                    headers = {"Range": f"bytes={chunk_range[0]}-{chunk_range[1]}"}
                    response = await client.get(url, headers=headers)
                    response.raise_for_status()
                    return response.content
            except httpx.HTTPError as e:
                log(f"Download failed with {e}. Retrying ({i+1}/{max_retries})...")
        raise httpx.HTTPError(f"Download failed after {max_retries} retries")


async def download_unzip(
    url,
    pasta_destino,
    unzip: bool,
):
    log(f"Baixando o arquivo {url}")
    content = await download(url)
    if unzip:
        save_path = os.path.join(pasta_destino, f"{os.path.basename(url)}.zip")

        with open(save_path, "wb") as fd:
            fd.write(content)
        try:
            with zipfile.ZipFile(save_path) as z:
                z.extractall(pasta_destino)
            log("Dados extraídos com sucesso!")
        except zipfile.BadZipFile:
            log(f"O arquivo {os.path.basename(url)} não é um arquivo ZIP válido.")
    else:
        save_path = os.path.join(pasta_destino, f"{os.path.basename(url)}")
        print(save_path)
        with open(save_path, "wb") as fd:
            fd.write(content)

    os.remove(save_path)


async def download_files_async(
    urls: List[str],
    save_paths: List[str],
    file_type: str,
) -> None:
    """
    Download files asynchronously.

    Parameters:
    - urls (Union[str, List[str]]): URL(s) of the file(s) to download.
    - save_paths (Union[str, List[str]]): Local path(s) to save the downloaded file(s).
    - file_type (str): Type of file to download ('csv', 'zip', 'ftp', etc.).
    - ftp_credentials (Optional[Tuple[str, str]]): FTP credentials (username, password).
    - max_concurrent (int): Maximum number of concurrent connections.

    Raises:
    - ValueError: If FTP credentials are required but not provided.
    """

    if isinstance(urls, str):
        urls = [urls]
        save_paths = [save_paths]
    tasks = []
    print(urls, save_paths)
    for url, save_path in zip(urls, save_paths):
        if file_type == "csv":
            tasks.append(download_unzip(url, save_path, unzip=False))
        elif file_type == "zip":
            tasks.append(download_unzip(url, save_path, unzip=True))
        else:
            return None
    await gather(*tasks)
