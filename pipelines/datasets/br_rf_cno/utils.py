import asyncio
import os

import httpx
from tqdm import tqdm

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
    Downloads a specific chunk of a file from a given URL and writes it to the specified file path.

    Args:
        client (httpx.AsyncClient): The HTTP client used for making requests.
        url (str): The URL of the file to be downloaded.
        start (int): The starting byte position of the chunk.
        end (int): The ending byte position of the chunk.
        filepath (str): The file path where the chunk will be written.
        semaphore (asyncio.Semaphore): A semaphore to limit the number of concurrent downloads.
        pbar (tqdm.tqdm): A progress bar to update the download progress.

    Returns:
        None
    """
    headers = {
        "Range": f"bytes={start}-{end}",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36",
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
    async with semaphore:
        response = await client.get(url, headers=headers, timeout=60.0)
        with open(filepath, "r+b") as f:
            f.seek(start)
            f.write(response.content)
            pbar.update(len(response.content))


async def download_file_async(root: str, url: str) -> None:
    """
    Downloads a file asynchronously from the given URL in chunks and saves it to the specified directory.

    Args:
        root (str): The root directory where the file will be saved.
        url (str): The URL of the file to be downloaded.

    Returns:
        None
    """
    filepath = f"{root}/data.zip"
    os.makedirs(root, exist_ok=True)

    log(f"----- Downloading files from {url}")

    async with httpx.AsyncClient() as client:
        response = await client.head(url, timeout=60.0)
        total_size = int(response.headers["content-length"])

    with open(filepath, "wb") as f:
        f.write(b"\0" * total_size)

    chunk_size = 1024 * 1024 * 16  # 16 MB chunks
    tasks: list[asyncio.Task] = []
    semaphore = asyncio.Semaphore(5)  # 5 downloads at the same time

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

    log("----- Downloading completed")
