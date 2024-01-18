# -*- coding: utf-8 -*-
"""
Schedules for br_cvm_oferta_publica_distribuicao
"""

from datetime import datetime, timedelta

from prefect.schedules import Schedule, filters
from prefect.schedules.clocks import IntervalClock

from pipelines.constants import constants

schedule_dia = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=1),
            start_date=datetime(2021, 1, 1, 6, 45),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_cvm_oferta_publica_distribuicao",
                "materialization_mode": "prod",
                "materialize_after_dump": True,
                "table_id": "dia",
                "update_metadata": True,
                "dbt_alias": True,
            },
        ),
    ],
    filters=[filters.is_weekday],
)
