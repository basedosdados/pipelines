"""
Schedules for botdosdados
"""

from datetime import datetime

from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock

from pipelines.constants import constants


every_hour = Schedule(
    clocks=[
        CronClock(
            cron="0 * * * *",  # 0 * * * * means at every hour.
            start_date=datetime(2021, 1, 1),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
        )
    ]
)
