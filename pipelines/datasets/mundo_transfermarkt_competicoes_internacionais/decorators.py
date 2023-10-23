# -*- coding: utf-8 -*-
"""
Custom decorators for pipelines.
"""
import asyncio

import requests
from bs4 import BeautifulSoup


def retry(content_function):
    async def wrapper(url, attempts=2, wait_time=2, **kwargs):
        content = None
        count = 0

        while content is None and count < attempts:
            headers = {
                "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36"
            }
            try:
                response = await asyncio.to_thread(
                    requests.get, url, headers=headers, timeout=100
                )
            except Exception:
                return None
            await asyncio.sleep(wait_time)
            soup = BeautifulSoup(response.text, "html.parser")
            try:
                content = content_function(soup, **kwargs)
            except Exception:
                if count == (attempts - 1):
                    # Could not get content
                    content = None
            count += 1
            # await asyncio.sleep(10)

        return content

    return wrapper
