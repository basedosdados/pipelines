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
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_mercadolivre_ofertas",
                "materialization_mode": "prod",
                "materialize_after_dump": True,
                "table_id": "item",
                "dbt_alias": False,
                "table_id_sellers": "vendedor",
                "materialize_after_dump_sellers": True,
            },
        )
    ],
)
