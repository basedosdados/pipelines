# -*- coding: utf-8 -*-
"""
Schedules for br_ibge_pnadc
"""

from datetime import datetime

from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock

from pipelines.constants import constants

every_day = Schedule(
    clocks=[
        CronClock(
            cron="30 4 * * *",  # everyday at 04:30
            start_date=datetime(2021, 1, 1, 15, 0),
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
