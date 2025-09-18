# -*- coding: utf-8 -*-
"""
Tasks for delete_flows
"""

from prefect import task
from prefect.client import Client

from pipelines.utils.utils import log


@task
def logs_stuff(string) -> Client:
    """
    Just logs some stuff
    """

    log(string)
