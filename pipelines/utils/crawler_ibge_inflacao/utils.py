# -*- coding: utf-8 -*-
"""
Schedules for ibge inflacao
"""

from datetime import timedelta, datetime
from prefect.schedules import Schedule, filters, adjustments
from prefect.schedules.clocks import IntervalClock
from pipelines.constants import constants


def generate_inflacao_clocks(parameters: dict):
    """
    generate ibge inflacao schedules
    """
    return Schedule(
        [
            IntervalClock(
                interval=timedelta(days=30),
                start_date=datetime(2021, 1, 1, 15, 5),
                labels=[
                    constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
                ],
                parameter_defaults=parameters,
            )
        ],
        filters=[filters.is_weekday],
        adjustments=[adjustments.next_weekday],
    )
