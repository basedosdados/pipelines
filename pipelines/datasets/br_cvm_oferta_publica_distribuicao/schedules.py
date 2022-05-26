# -*- coding: utf-8 -*-
"""
Schedules for br_cvm_oferta_publica_distribuicao
"""

from datetime import timedelta, datetime
from prefect.schedules import Schedule, filters
from prefect.schedules.clocks import IntervalClock
from pipelines.constants import constants

schedule_dia = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=1),
            start_date=datetime(2021, 1, 1),
            filters=[filters.is_weekday]
            labels=[
                constants.BASEDOSDADOS_DEV_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_cvm_oferta_publica_distribuicao",
                "materialization_mode": "dev",
                "materialize after dump": True,
                "table_id": "dia",
            },
        ),
    ],
)
