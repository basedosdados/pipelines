# -*- coding: utf-8 -*-
"""
Schedules for br_ms_sim
"""
from datetime import datetime
from prefect.schedules import Schedule, adjustments, filters
from prefect.schedules.clocks import CronClock
from pipelines.constants import constants

everyday_sinan_microdados = Schedule(
    clocks=[
        CronClock(
            cron="0 9 1 * *",  # every 1st day of the month at 09:00
            start_date=datetime(2024, 1, 1, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_ms_sim",
                "table_id": "microdados",
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