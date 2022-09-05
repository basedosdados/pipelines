# -*- coding: utf-8 -*-
"""
Schedules for br_fgv_igp
"""

from datetime import timedelta, datetime

from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock

from pipelines.constants import constants

igp_di_mes = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=30),
            start_date=datetime(2021, 1, 1),
            labels=[
                constants.BASEDOSDADOS_DEV_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_fgv_igp",
                "table_id": "igp_di_mes",
                "indice": "IGPDI",
                "periodo": "mensal",
                "materialization_mode": "dev",
                "materialize_after_dump": True,
                "dbt_alias": True,
            },
        ),
    ]
)
