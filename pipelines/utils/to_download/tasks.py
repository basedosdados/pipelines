# -*- coding: utf-8 -*-
"""
Tasks for to_download
"""


import asyncio
import os
from typing import List

from prefect import task

from pipelines.utils.to_download.utils import download_files_async


@task  # noqa
def to_download(
    url: List[str],
    save_path: List[str],
    file_type: str,
    params=None,
    credentials=None,
    auth_method=None,
) -> str:
    """
    Downloads data from an API and saves it to a local file.

    Args:
        urls (Union[str, List[str]]): URL(s) of the file(s) to download.
        save_paths (Union[str, List[str]]): Local path(s) to save the downloaded file(s).
        file_type (str): Type of file to download ('csv', 'zip', 'ftp', etc.).
        params (dict, optional): Additional parameters to be included in the API request. Defaults to None.
        credentials (str or tuple, optional): The credentials to be used for authentication. Defaults to None.
        auth_method (str, optional): The authentication method to be used. Valid values are "bearer" and "basic". Defaults to "bearer".

    Returns:
        list: The files path's of the downloaded files.

    """
    asyncio.run(
        download_files_async(
            url,
            save_path,
            file_type,
            params=params,
            credentials=credentials,
            auth_method=auth_method,
        )
    )
    if isinstance(url, str):
        url = [url]
        save_path = [save_path]
    base_name = [os.path.basename(i) for i in url]
    return [os.path.join(x, y) for x, y in zip(save_path, base_name)]
