# -*- coding: utf-8 -*-
"""
Schedules for br_anp_precos_combustiveis
"""

from datetime import timedelta, datetime
from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock
from pipelines.constants import constants

every_week_anp_microdados = Schedule(
    clocks=[
        CronClock(
            cron="0 10 * * *",
            start_date=datetime(2021, 1, 1),
            labels=[constants.BASEDOSDADOS_PROD_AGENT_LABEL.value],
            parameter_defaults={
                "update_metadata": True,
                "dbt_alias": True,
                "materialize_after_dump": True,
                "materialization_mode": "prod",
                "table_id": "microdados",
                "dataset_id": "br_anp_precos_combustiveis",
            },
        ),
    ],
)
