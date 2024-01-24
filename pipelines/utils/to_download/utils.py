# -*- coding: utf-8 -*-
import asyncio
import io
import zipfile
from ftplib import FTP
from typing import List
from urllib.parse import urlparse

import aiohttp
import requests


async def download_csv(
    session: aiohttp.ClientSession,
    url: str,
    save_path: str,
    semaphore: asyncio.Semaphore,
) -> None:
    """
    Download a CSV file asynchronously with semaphore.

    Parameters:
    - session (aiohttp.ClientSession): Aiohttp client session.
    - url (str): URL of the CSV file to download.
    - save_path (str): Local path to save the downloaded CSV file.
    - semaphore (asyncio.Semaphore): Semaphore to control concurrent connections.
    """
    async with semaphore:
        async with session.get(url) as response:
            with open(save_path, "wb") as f:
                while chunk := await response.content.read(1024):
                    f.write(chunk)


async def download_zip(
    session: aiohttp.ClientSession,
    url: str,
    save_path: str,
    semaphore: asyncio.Semaphore,
) -> None:
    """
    Download and extract a ZIP file asynchronously with semaphore.

    Parameters:
    - session (aiohttp.ClientSession): Aiohttp client session.
    - url (str): URL of the ZIP file to download.
    - save_path (str): Local path to save the extracted files.
    - semaphore (asyncio.Semaphore): Semaphore to control concurrent connections.
    """
    async with semaphore:
        async with session.get(url) as response:
            with zipfile.ZipFile(io.BytesIO(await response.content.read()), "r") as z:
                z.extractall(save_path)


async def download_ftp(
    url: str, save_path: str, username: str, password: str, semaphore: asyncio.Semaphore
) -> None:
    """
    Download a file from an FTP server with semaphore.

    Parameters:
    - url (str): FTP URL of the file to download.
    - save_path (str): Local path to save the downloaded file.
    - username (str): FTP username.
    - password (str): FTP password.
    - semaphore (asyncio.Semaphore): Semaphore to control concurrent connections.
    """
    async with semaphore:
        parsed_url = urlparse(url)
        ftp = FTP(parsed_url.netloc)
        ftp.login(user=username, passwd=password)
        with open(save_path, "wb") as f:
            ftp.retrbinary(f"RETR {parsed_url.path}", f.write)


async def download_files_async(
    urls: List[str],
    save_paths: List[str],
    file_type: str,
    ftp_credentials=None,
    max_concurrent=5,
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
    semaphore = asyncio.Semaphore(max_concurrent)

    if isinstance(urls, str):
        urls = [urls]
        save_paths = [save_paths]

    async with aiohttp.ClientSession() as session:
        tasks = []
        for url, save_path in zip(urls, save_paths):
            if file_type == "csv":
                tasks.append(download_csv(session, url, save_path, semaphore))
            elif file_type == "zip":
                tasks.append(download_zip(session, url, save_path, semaphore))
            elif file_type == "ftp":
                if ftp_credentials:
                    username, password = ftp_credentials
                    tasks.append(
                        download_ftp(url, save_path, username, password, semaphore)
                    )
                else:
                    raise ValueError("FTP credentials required for FTP download")
        await asyncio.gather(*tasks)
