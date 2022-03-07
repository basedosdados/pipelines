"""
Schedules for br_ibge_ipca
"""

from datetime import timedelta, datetime
from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock
from pipelines.constants import constants

every_day = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=1),
            start_date=datetime(2021, 1, 1),
            labels=[
                constants.BASEDOSDADOS_DEV_AGENT_LABEL.value,
            ]
        )
    ]
)
