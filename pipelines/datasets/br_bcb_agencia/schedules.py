# -*- coding: utf-8 -*-
"""
Schedules for br_bcb_agencia
"""

from datetime import datetime
from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock
from pipelines.constants import constants

every_month_agencia = Schedule(
    clocks=[
        CronClock(
            cron="00 15 5 * *",
            start_date=datetime(2007, 9, 1),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_bcb_agencia",
                "table_id": "agencia",
                "materialization_mode": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
            },
        )
    ]
)
