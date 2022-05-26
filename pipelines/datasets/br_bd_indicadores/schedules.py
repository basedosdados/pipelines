# -*- coding: utf-8 -*-
"""
Schedules for bd_tweet_data
"""

from datetime import timedelta, datetime
from prefect.schedules import Schedule, filters, adjustments
from prefect.schedules.clocks import IntervalClock
from pipelines.constants import constants

every_week = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(weeks=1),
            start_date=datetime(2021, 1, 1, 17, 35),
            labels=[
                constants.BASEDOSDADOS_DEV_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_bd_indicadores",
                "materialization_mode": "dev",
                "materialize after dump": True,
                "table_id": "twitter_metrics_agg",
            },
        ),
    ],
    filters=[filters.is_weekday],
    adjustments=[adjustments.next_weekday],
)

every_day = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=1),
            start_date=datetime(2022, 5, 18, 16, 24),
            labels=[
                constants.BASEDOSDADOS_DEV_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_bd_indicadores",
                "materialization_mode": "dev",
                "materialize after dump": True,
                "table_id": "twitter_metrics",
            },
        ),
    ],
    filters=[filters.is_weekday],
)
