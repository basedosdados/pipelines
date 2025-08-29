# -*- coding: utf-8 -*-
"""
Schedules for br_bcb_estban
"""

# ESTBAN data is released every month, lagging 60 days during january until november
# and 90 days during december (...)

from datetime import datetime

from prefect.schedules import Schedule, adjustments, filters
from prefect.schedules.clocks import CronClock

from pipelines.constants import constants

every_month_agencia = Schedule(
    clocks=[
        CronClock(
            cron="0 22 25-31 * *",  # At 22:00, at the 7 last days the month, on weekdays 1-5
            start_date=datetime(2025, 7, 30, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_bcb_estban",
                "table_id": "agencia",
                "target": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "update_metadata": True,
            },
        )
    ],
    filters=[filters.is_weekday],
    adjustments=[adjustments.next_weekday],
)


every_month_municipio = Schedule(
    clocks=[
        CronClock(
            cron="0 22 25-31 * *",  # At 22:00, at the 7 last days the month, on weekdays 1-5
            start_date=datetime(2025, 7, 30, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_bcb_estban",
                "table_id": "municipio",
                "target": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "update_metadata": True,
            },
        )
    ],
    filters=[filters.is_weekday],
    adjustments=[adjustments.next_weekday],
)
