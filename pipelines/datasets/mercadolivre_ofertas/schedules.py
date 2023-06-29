# -*- coding: utf-8 -*-
"""
Schedules for br_poder360_pesquisas
"""

from datetime import datetime, timedelta

from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock

from pipelines.constants import constants


every_day_item = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=1),
            start_date=datetime(2021, 1, 1, 15, 0),
            labels=[
                constants.BASEDOSDADOS_DEV_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "mercadolivre_ofertas",
                "materialization_mode": "dev",
                "materialize after dump": False,
                "table_id": "item",
                "dbt_alias": False,
            },
        )
    ],
)
