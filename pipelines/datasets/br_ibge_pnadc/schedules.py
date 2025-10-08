"""
Schedules for br_ibge_pnadc
"""

from datetime import datetime

from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock

from pipelines.constants import constants

every_trimester = Schedule(
    clocks=[
        CronClock(
            cron="0 5 15-31 2,5,8,11 *",  # At 05:00 AM, between day 15 and 31 of the month, only in February, May, August, and November
            start_date=datetime(2021, 2, 15, 5, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_ibge_pnadc",
                "table_id": "microdados",
                "target": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "update_metadata": True,
            },
        )
    ],
)
