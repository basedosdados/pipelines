# -*- coding: utf-8 -*-
"""
Schedules for bd_tweet_data
"""

from datetime import datetime, timedelta

from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock

from pipelines.constants import constants

every_day_prefect_flow_runs = Schedule(
    clocks=
    
    
    [
        CronClock(
            cron="10 2 * * *",  # every day at 2:10 UTC
            start_date=datetime(2024, 1, 12),
            labels=[constants.BASEDOSDADOS_PROD_AGENT_LABEL.value],
            parameter_defaults={
                "dataset_id": "br_bd_metadados",
                "table_id": "prefect_flow_runs",
                "materialization_mode": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
            },
        ),
    ]
)

every_day_prefect_flows = Schedule(
    clocks=
    [
        CronClock(
            cron="10 2 * * *",  # every day at 2:10 UTC
            start_date=datetime(2024, 1, 12),
            labels=[constants.BASEDOSDADOS_PROD_AGENT_LABEL.value],
            parameter_defaults={
                "dataset_id": "br_bd_metadados",
                "table_id": "prefect_flows",
                "materialization_mode": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
            },
        ),
    ],
)
