# -*- coding: utf-8 -*-
"""
Schedules for br_bcb_estban
"""

# ESTBAN data is released every month, lagging 60 days during january until november
# and 90 days during december (...) qnd schedular

from datetime import timedelta, datetime
from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock
from pipelines.constants import constants

every_two_weeks = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(weeks=2),
            start_date=datetime(2021, 1, 1),
            labels=[
                constants.DATASETS_AGENT_LABEL.value,
            ],
        ),
    ]
)
