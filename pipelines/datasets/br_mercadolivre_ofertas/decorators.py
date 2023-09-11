# -*- coding: utf-8 -*-
"""
Custom decorators for pipelines.
"""
import asyncio
from bs4 import BeautifulSoup
import requests


def retry(content_function):
    """Decorator function that retries an asynchronous content retrieval function.

    Args:
        content_function (callable): An asynchronous function that takes BeautifulSoup object and additional keyword arguments as parameters, and returns the desired content.

    Returns:
        callable: A wrapper function that retries the content retrieval function.

    Raises:
        None

    Example:
        @retry
        async def get_title(soup):
            # Retrieves the title from a BeautifulSoup object
            title = soup.title.string
            return title

        # Usage
        url = 'https://example.com'
        title = await get_title(url)
    """

    async def wrapper(url, attempts=2, wait_time=2, **kwargs):
        content = None
        count = 0

        while content is None and count < attempts:
            # print(f'Attempt {count + 1} of {attempts}')
            headers = {"User-Agent": "Chrome/39.0.2171.95"}
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
