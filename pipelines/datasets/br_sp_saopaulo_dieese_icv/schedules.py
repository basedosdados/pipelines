# -*- coding: utf-8 -*-
"""
Schedules for br_sp_saopaulo_dieese_icv
"""

from datetime import timedelta, datetime
from prefect.schedules import Schedule, filters, adjustments
from prefect.schedules.clocks import IntervalClock
from pipelines.constants import constants

every_month = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=30),
            start_date=datetime(2021, 1, 1, 14, 32),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_sp_saopaulo_dieese_icv",
                "materialization_mode": "prod",
                "materialize after dump": True,
                "table_id": "mes",
                "dbt_alias": False,
            },
        )
    ],
    filters=[filters.is_weekday],
    adjustments=[adjustments.next_weekday],
)
