# -*- coding: utf-8 -*-
"""
Schedules for br_fgv_igp
"""

from datetime import timedelta, datetime
from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock
from pipelines.constants import constants

every_month = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(day=30),
            start_date=datetime(2021, 1, 1),
            labels=[
                constants.BASEDOSDADOS_DEV_AGENT_LABEL.value,
            ],
        ),
    ]
)
