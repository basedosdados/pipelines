# -*- coding: utf-8 -*-
"""
Schedules for br_bcb_agencia
"""

from datetime import datetime
from prefect.schedules import Schedule, filters, adjustments
from prefect.schedules.clocks import CronClock
from pipelines.constants import constants

every_month_agencia = Schedule(
    clocks=[
        CronClock(
            cron="@monthly",
            start_date=datetime(2023, 8, 5, 0, 0),
            labels=[
                constants.BASEDOSDADOS_DEV_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_bcb_agencia",
                "table_id": "agencia",
                "materialization_mode": "prod",
                "materialize after dump": True,
                "dbt_alias": False,
            },
        )
    ],
    filters=[filters.is_weekday],
    adjustments=[adjustments.next_weekday],
)
