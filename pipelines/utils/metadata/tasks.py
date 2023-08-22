# -*- coding: utf-8 -*-
"""
Tasks for metadata
"""

from prefect import task


@task  # noqa
def say_hello(name: str = "World") -> str:
    """
    Greeting task.
    """
    return f"Hello, {name}!"
