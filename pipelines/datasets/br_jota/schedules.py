# -*- coding: utf-8 -*-
"""
Schedules for br_jota
"""

from datetime import datetime

from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock

from pipelines.constants import constants


schedule_br_jota_2024 = Schedule(
    clocks=[
        CronClock(
            cron="30 4 * * *",
            start_date=datetime(2024, 9, 1, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PERGUNTAS_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_jota",
                "materialization_mode": "dev",
                "dbt_alias": True,
            },
        ),
    ],
)
