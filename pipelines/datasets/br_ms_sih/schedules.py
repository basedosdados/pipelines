# -*- coding: utf-8 -*-
"""
Schedules for br_ms_sih
"""
from datetime import datetime, timedelta

from prefect.schedules import Schedule, adjustments, filters
from prefect.schedules.clocks import CronClock
from pipelines.constants import constants


everyday_sih_servicos_profissionais = Schedule(
    clocks=[
        CronClock(
            cron="30 3 * * *",  # every day at 03:30
            start_date=datetime(2024, 1, 1, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_ms_sih",
                "table_id": "servicos_profissionais",
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

everyday_sih_aihs_reduzidas = Schedule(
    clocks=[
        CronClock(
            cron="30 6 * * *",  # every day at 06:30
            start_date=datetime(2024, 1, 1, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_ms_sih",
                "table_id": "aihs_reduzidas",
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
