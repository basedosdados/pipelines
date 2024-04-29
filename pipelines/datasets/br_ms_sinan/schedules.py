# -*- coding: utf-8 -*-
"""
Schedules for br_ms_sinan
"""
from datetime import datetime

from prefect.schedules import Schedule, adjustments, filters
from prefect.schedules.clocks import CronClock
from pipelines.constants import constants


everyday_sinan_microdados = Schedule(
    clocks=[
        CronClock(
            cron="45 4 * * *",  # every day at 03:30
            start_date=datetime(2024, 1, 1, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_ms_sinan",
                "table_id": "microdados_dengue",
                "materialization_mode": "prod",
                "materialize_after_dump": True,
                "update_metadata": True,
                "dbt_alias": True,
            },
        )
    ],
    filters=[filters.is_weekday],
    adjustments=[adjustments.next_weekday],
)