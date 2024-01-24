# -*- coding: utf-8 -*-
"""
Tasks for to_download
"""


import asyncio

from prefect import task

from pipelines.utils.to_download.utils import download_files_async


@task  # noqa
def to_download(url, save_path, file_type) -> str:
    """ """
    asyncio.run(download_files_async(url, save_path, file_type))
