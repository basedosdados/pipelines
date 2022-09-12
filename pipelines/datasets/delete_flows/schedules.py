# -*- coding: utf-8 -*-
"""
Schedules for the daily cleanup flow.
"""

from datetime import timedelta, datetime

from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock
import pytz

from pipelines.constants import constants


daily_at_3am = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=1),
            start_date=datetime(
                2021, 1, 1, 3, tzinfo=pytz.timezone("America/Sao_Paulo")
            ),
            labels=[
                constants.BASEDOSDADOS_DEV_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "days_old": 15,
            },
        ),
    ]
)
