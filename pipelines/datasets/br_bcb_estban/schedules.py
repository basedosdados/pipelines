# -*- coding: utf-8 -*-
"""
Schedules for br_bcb_estban
"""

# ESTBAN data is released every month, lagging 60 days during january until november
# and 90 days during december (...)

from datetime import datetime
from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock
from pipelines.constants import constants


every_month_agencia = Schedule(
    clocks=[
        CronClock(
            cron="30 19 * * 2",  # 15th day of every month at 15:00
            start_date=datetime.today(),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_bcb_estban",
                "table_id": "agencia",
                "materialization_mode": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
            },
        )
    ]
)


every_month_municipio = Schedule(
    clocks=[
        CronClock(
            cron="30 19 * * 2",  # 15th day of every month at 15:00
            start_date=datetime.today(),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_bcb_estban",
                "table_id": "municipio",
                "materialization_mode": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
            },
        )
    ]
)
