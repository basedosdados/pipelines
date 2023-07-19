# -*- coding: utf-8 -*-
"""
Schedules for br_anatel_telefonia_movel
"""
from datetime import timedelta, datetime
from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock
from pipelines.constants import constants
from prefect.schedules.clocks import CronClock


every_month_anatel = Schedule(
    clocks=[
        CronClock(
            cron="50 16 28 * *",  # At 17:50 on day-of-month 28
            start_date=datetime(2021, 3, 31, 17, 11),
            labels=[
                constants.BASEDOSDADOS_DEV_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_anatel_telefonia_movel",
                "table_id": "microdados",
                "materialization_mode": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "update_metadata": True,
            },
        ),
    ],
)
