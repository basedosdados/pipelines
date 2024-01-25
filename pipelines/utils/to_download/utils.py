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


async def unzip_file(zip_path, extract_to):
    log(f"Extraindo dados do arquivo {os.path.basename(zip_path)}")

    try:
        with zipfile.ZipFile(zip_path) as z:
            z.extractall(extract_to)
        log("Dados extraídos com sucesso!")
    except zipfile.BadZipFile:
        log(f"O arquivo {os.path.basename(zip_path)} não é um arquivo ZIP válido.")
    except zipfile.LargeZipFile:
        log(
            f"O arquivo ZIP {os.path.basename(zip_path)} é muito grande para ser processado."
        )
    except zipfile.error as e:
        log(f"Erro ao extrair o arquivo ZIP {os.path.basename(zip_path)}: {str(e)}")
    os.remove(zip_path)


async def get_from_api(
    url: str,
    max_retries: int,
    timeout: int,
    semaphore: Semaphore,
    params=None,
    credentials=None,
    auth_method=None,
):
    """"""
    async with semaphore:
        # log(f"Downloading chunk {chunk_range[0]}-{chunk_range[1]}")
        params = {} if params is None else params
        for i in range(max_retries):
            try:
                if auth_method == "bearer":
                    async with httpx.AsyncClient(timeout=timeout) as client:
                        headers = {
                            "Authorization": f"Bearer {credentials}",
                        }
                        response = await client.get(url, headers=headers, params=params)
                elif auth_method == "basic":
                    async with httpx.AsyncClient(timeout=timeout) as client:
                        response = await client.get(
                            url, params=params, auth=credentials
                        )
                else:
                    async with httpx.AsyncClient(timeout=timeout) as client:
                        response = await client.get(url, params=params)
                response.raise_for_status()
                return response.content
            except httpx.HTTPError as e:
                log(f"Download failed with {e}. Retrying ({i+1}/{max_retries})...")
        raise httpx.HTTPError(f"Download failed after {max_retries} retries")


async def get_in_chunks(
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


async def download(
    url: str,
    chunk_size: int = 2**20,
    max_retries: int = 32,
    max_parallel: int = 16,
    timeout: int = 3 * 60 * 1000,
    params=None,
    credentials=None,
    auth_method=None,
    file_type: str = None,
) -> bytes:
    request_head = httpx.head(url)

    assert request_head.status_code == 200
    semaphore = Semaphore(max_parallel)
    if file_type == "json":
        return b"".join(
            await gather(
                get_from_api(
                    url,
                    max_retries,
                    timeout,
                    semaphore,
                    params,
                    credentials,
                    auth_method,
                )
            )
        )
    else:
        assert request_head.headers["accept-ranges"] == "bytes"
        content_length = int(request_head.headers["content-length"])
        log(
            f"Downloading {url} with {content_length} bytes / {chunk_size} chunks and {max_parallel} parallel downloads"
        )
        tasks = [
            get_in_chunks(
                url,
                (start, end),
                max_retries,
                timeout,
                semaphore,
                params,
                auth_method,
                credentials,
            )
            for start, end in chunk_range(content_length, chunk_size)
        ]

        return b"".join(await gather(*tasks))


async def download_from_url(
    url,
    save_path,
    unzip: bool,
    params=None,
    credentials=None,
    auth_method=None,
    file_type=None,
):
    log(f"Baixando o arquivo {url}")
    content = await download(
        url,
        params=params,
        credentials=credentials,
        auth_method=auth_method,
        file_type=file_type,
    )

    base_name = os.path.basename(url) + (".json" if file_type == "json" else "")
    full_path = os.path.join(save_path, base_name)

    with open(full_path, "wb") as fd:
        fd.write(content)

    if unzip:
        await unzip_file(full_path, save_path)
    else:
        log(f"Arquivo {base_name} salvo em {save_path}")


async def download_files_async(
    urls: List[str],
    save_paths: List[str],
    file_type: str,
    params=None,
    credentials=None,
    auth_method=None,
) -> None:
    """
    Download files asynchronously.
    """

    if isinstance(urls, str):
        urls = [urls]
        save_paths = [save_paths]
    tasks = []
    for url, save_path in zip(urls, save_paths):
        if file_type == "csv":
            tasks.append(
                download_from_url(
                    url,
                    save_path,
                    unzip=False,
                    params=params,
                    credentials=credentials,
                    auth_method=auth_method,
                )
            )
        elif file_type == "zip":
            tasks.append(
                download_from_url(
                    url,
                    save_path,
                    unzip=True,
                    params=params,
                    credentials=credentials,
                    auth_method=auth_method,
                )
            )
        elif file_type == "json":
            tasks.append(
                download_from_url(
                    url,
                    save_path,
                    unzip=False,
                    params=params,
                    credentials=credentials,
                    auth_method=auth_method,
                    file_type="json",
                )
            )
        else:
            raise ValueError(f"Invalid file_type: {file_type}")
    await gather(*tasks)
