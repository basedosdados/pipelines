# -*- coding: utf-8 -*-
"""
Schedules for botdosdados
"""

from datetime import datetime

from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock

from pipelines.constants import constants


every_day = Schedule(
    clocks=[
        CronClock(
            cron="0 15 * * *",  # 0 * * * * means at every day at 12:00 Brazil.
            start_date=datetime(2021, 1, 1),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
        )
    ]
)
