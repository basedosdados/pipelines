# -*- coding: utf-8 -*-
"""
Tasks for delete_flows
"""

from prefect import task

from pipelines.utils.utils import log


@task
def logs_stuff(string) -> None:
    """
    Just logs some stuff
    """

    log(string)
