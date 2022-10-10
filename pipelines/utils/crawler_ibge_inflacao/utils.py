# -*- coding: utf-8 -*-
"""
Schedules for ibge inflacao
"""
# pylint: disable=arguments-differ
import ssl
from datetime import datetime

import requests
from prefect.schedules import Schedule, filters, adjustments
from prefect.schedules.clocks import CronClock
import urllib3

from pipelines.constants import constants


def generate_inflacao_clocks(parameters: dict):
    """
    generate ibge inflacao schedules
    """
    return Schedule(
        clocks=[
        CronClock(
            cron="0 18 12 * *", #day 12 of every month at 6 pm
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
