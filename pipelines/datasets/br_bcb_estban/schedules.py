# -*- coding: utf-8 -*-
"""
Schedules for br_bcb_estban
"""

# ESTBAN data is released every month, lagging 60 days during january until november
# and 90 days during december (...)

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
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_bcb_estban",
                "table_id": "agencia",
                "materialization_mode": "prod",
                "materialize after dump": True,
                "dbt_alias": False,
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
            cron="@monthly",
            start_date=datetime(2023, 8, 5, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_bcb_estban",
                "table_id": "municipio",
                "materialization_mode": "prod",
                "materialize after dump": True,
                "dbt_alias": False,
                "update_metadata": True,
            },
        )
    ],
    filters=[filters.is_weekday],
    adjustments=[adjustments.next_weekday],
)
