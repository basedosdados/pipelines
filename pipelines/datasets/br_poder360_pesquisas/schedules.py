# -*- coding: utf-8 -*-
"""
Schedules for br_poder360_pesquisas
"""

from datetime import datetime, timedelta

from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock

from pipelines.constants import constants

every_monday_thursday = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=1),
            start_date=datetime(2021, 1, 1, 15, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_poder360_pesquisas",
                "materialization_mode": "prod",
                "materialize after dump": True,
                "table_id": "microdados",
                "dbt_alias": False,
            },
        )
    ],
)
