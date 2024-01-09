# -*- coding: utf-8 -*-
"""
Schedules for bd_tweet_data
"""

from datetime import datetime, timedelta

from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock

from pipelines.constants import constants

every_day_prefect = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=1),
            start_date=datetime(2022, 9, 20, 10, 00),
            labels=[
                constants.BASEDOSDADOS_DEV_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_bd_metadados",
                "table_id": "organizations",
                "materialization_mode": "dev",
                "materialize_after_dump": True,
                "dbt_alias": False,
            },
        ),
    ],
)
