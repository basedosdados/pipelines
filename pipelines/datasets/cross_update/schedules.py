# -*- coding: utf-8 -*-
"""
Schedules for br_tse_eleicoes
"""

from datetime import timedelta, datetime

from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock

from pipelines.constants import constants

schedule_nrows = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=7),
            start_date=datetime(2021, 1, 1, 9, 45),
            labels=[
                str(constants.BASEDOSDADOS_PROD_AGENT_LABEL.value),
            ],
            parameter_defaults={
                "dump_to_gcs": True,
                "days": 7,
                "mode": "prod",
                "page_size": 100,
            },
        ),
    ],
)
