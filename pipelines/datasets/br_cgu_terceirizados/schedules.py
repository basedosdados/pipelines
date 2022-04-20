"""
Schedules for br_cgu_terceirizados
"""

from datetime import datetime
from dateutil.rrule import rrule, MONTHLY
from prefect.schedules import Schedule
from prefect.schedules.clocks import RRuleClock
from pipelines.constants import constants


every_four_months = Schedule(
    clocks=[
        RRuleClock(
            rrule(freq=MONTHLY, count=4),
            start_date=datetime(2021, 1, 1),
            labels=[
                constants.BASEDOSDADOS_DEV_AGENT_LABEL.value
                ]
            )
        ]
    )
