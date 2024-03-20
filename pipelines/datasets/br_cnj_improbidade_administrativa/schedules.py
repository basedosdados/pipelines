# -*- coding: utf-8 -*-
"""
Schedules for br_cnj_improbidade_administrativa
"""

from datetime import datetime

from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock

from pipelines.constants import constants

every_month = Schedule(
    clocks=[
        CronClock(
            cron="0 7 * * 1",  # At 07:00 on Monday.
            start_date=datetime(2023, 8, 2, 10, 30),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_cnj_improbidade_administrativa",
                "table_id": "condenacao",
                "materialization_mode": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
            },
        ),
    ]
)
