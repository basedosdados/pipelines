# -*- coding: utf-8 -*-
"""
Schedules for br_poder360_pesquisas
"""

from datetime import datetime

from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock

from pipelines.constants import constants

every_monday_thursday = Schedule(
    clocks=[
        CronClock(
            cron="42 3 * * *",  # every day at 03:42
            start_date=datetime(2023, 9, 1, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_poder360_pesquisas",
                "materialization_mode": "prod",
                "materialize after dump": True,
                "table_id": "microdados",
                "dbt_alias": False,
            },
        )
    ],
)
