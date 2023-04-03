# -*- coding: utf-8 -*-
"""
Schedules for br_bcb_estban
"""

# ESTBAN data is released every month, lagging 60 days during january until november
# and 90 days during december (...)

from datetime import timedelta, datetime
from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock
from pipelines.constants import constants


every_month_agencia = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=30),
            start_date=datetime(1988, 7, 1),
            labels=[
                constants.BASEDOSDADOS_DEV_AGENT_LABEL.value,
            ],
        )
    ]
)


every_month_municipio = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=30),
            start_date=datetime(1988, 7, 1),
            labels=[
                constants.BASEDOSDADOS_DEV_AGENT_LABEL.value,
            ],
        )
    ]
)
