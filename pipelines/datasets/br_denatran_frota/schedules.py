# -*- coding: utf-8 -*-
"""
Schedules for br_denatran_frota
"""
from datetime import datetime

from prefect.schedules import Schedule, adjustments, filters
from prefect.schedules.clocks import CronClock

from pipelines.constants import constants

every_month_municipio = Schedule(
    clocks=[
        CronClock(
            cron="20 21 10-30 * *",  # At 21:20 on every day-of-month from 10 through 30.
            start_date=datetime(2023, 11, 22, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_denatran_frota",
                "table_id": "municipio_tipo",
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


every_month_uf = Schedule(
    clocks=[
        CronClock(
            cron="0 21 10-30 * *",  # At 21:00 on every day-of-month from 10 through 30.
            start_date=datetime(2023, 11, 22, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_denatran_frota",
                "table_id": "uf_tipo",
                "materialization_mode": "prod",
                "materialize after dump": True,
                "dbt_alias": True,
                "update_metadata": True,
            },
        )
    ],
    filters=[filters.is_weekday],
    adjustments=[adjustments.next_weekday],
)
