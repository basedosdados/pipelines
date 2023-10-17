# -*- coding: utf-8 -*-
"""
Schedules for ibge inflacao
"""
# pylint: disable=arguments-differ
import ssl
from datetime import datetime

import basedosdados as bd
import requests
import urllib3
from prefect.schedules import Schedule, adjustments, filters
from prefect.schedules.clocks import CronClock

from pipelines.constants import constants
from pipelines.utils.utils import log


def generate_inflacao_clocks(parameters: dict):
    """
    generate ibge inflacao schedules
    """
    return Schedule(
        clocks=[
            CronClock(
                cron="0 18 12 * *",  # day 12 of every month at 6 pm
                start_date=datetime(2021, 1, 1, 15, 0),
                labels=[
                    constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
                ],
                parameter_defaults=parameters,
            )
        ],
        filters=[filters.is_weekday],
        adjustments=[adjustments.next_weekday],
    )


# class to deal with ssl context. See more at:https://github.com/psf/requests/issues/4775
class CustomHttpAdapter(requests.adapters.HTTPAdapter):
    """Transport adapter" that allows us to use custom ssl_context."""

    def __init__(self, ssl_context=None, **kwargs):
        """Initiate the class with the ssl context"""
        self.ssl_context = ssl_context
        super().__init__(**kwargs)

    def init_poolmanager(self, connections, maxsize, block=False):
        """Initiate the pool manager"""
        self.poolmanager = urllib3.poolmanager.PoolManager(
            num_pools=connections,
            maxsize=maxsize,
            block=block,
            ssl_context=self.ssl_context,
        )


def get_legacy_session():
    """Get the session with the ssl context"""
    ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
    ctx.options |= 0x4  # OP_LEGACY_SERVER_CONNECT
    session = requests.session()
    session.mount("https://", CustomHttpAdapter(ctx))
    return session


def extract_last_date(
    dataset_id: str,
    table_id: str,
    billing_project_id: str,
) -> str:
    """
    Extracts the last update date of a given dataset table.

    Args:
        dataset_id (str): The ID of the dataset.
        table_id (str): The ID of the table.
        billing_project_id (str): The billing project ID.

    Returns:
        str: The last update date in the format 'yyyy-mm-dd'.

    Raises:
        Exception: If an error occurs while extracting the last update date.
    """

    query_bd = f"""
    SELECT MAX(DATE(CAST(ano AS INT64),CAST(mes AS INT64),1)) as max_date
    FROM
    `{billing_project_id}.{dataset_id}.{table_id}`
    """

    t = bd.read_sql(
        query=query_bd,
        billing_project_id=billing_project_id,
        from_file=True,
    )

    data = t["max_date"][0]
    data = data.strftime("%Y-%m")

    return data
