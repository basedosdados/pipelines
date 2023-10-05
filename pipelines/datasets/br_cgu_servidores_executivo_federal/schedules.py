# -*- coding: utf-8 -*-
"""
Schedules for br_cgu_servidores_executivo_federal
"""

from datetime import datetime

from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock

from pipelines.constants import constants
from pipelines.datasets.br_cgu_servidores_executivo_federal.constants import (
    constants as cgu_constants,
)

every_month = Schedule(
    clocks=[
        CronClock(
            cron="0 17 15 * *",  # At 17:00 on day-of-month 15
            start_date=datetime(2023, 9, 26),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_cgu_servidores_executivo_federal",
                "table_id": list(cgu_constants.TABLES.value.keys()),
                "materialization_mode": "dev",
                "materialize_after_dump": True,
                "dbt_alias": True,
            },
        ),
    ]
)
