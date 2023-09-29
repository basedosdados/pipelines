# -*- coding: utf-8 -*-
"""
Schedules for br_bcb_agencia
"""

from datetime import datetime

from prefect.schedules import Schedule, adjustments, filters
from prefect.schedules.clocks import CronClock

from pipelines.constants import constants

every_month_agencia = Schedule(
    clocks=[
        CronClock(
            cron="@monthly",
            start_date=datetime(2023, 9, 5, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_bcb_agencia",
                "table_id": "agencia",
                "materialization_mode": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "update_metadata": True,
            },
        )
    ],
    filters=[filters.is_weekday],
    adjustments=[adjustments.next_weekday],
)
