# -*- coding: utf-8 -*-
"""
Schedules for metadata flows
"""


from datetime import datetime
from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock
from pipelines.constants import constants


every_day_quality_checks = Schedule(
    clocks=[
        CronClock(
            cron="0 21 * * *",  # At 23:00 on every day-of-week from Monday through Friday.
            start_date=datetime(2024, 3, 20, 12, 26),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
        ),
    ],
)
