# -*- coding: utf-8 -*-
"""
Schedules for br_sp_saopaulo_dieese_icv
"""

from datetime import timedelta, datetime
from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock
from pipelines.constants import constants

every_month = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=30),
            start_date=datetime(2021, 1, 1),
            labels=[
                constants.BASEDOSDADOS_DEV_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_bd_indicadores",
                "materialization_mode": "dev",
                "materialize after dump": True,
                "table_id": "mes",
            },
        )
    ],
)
