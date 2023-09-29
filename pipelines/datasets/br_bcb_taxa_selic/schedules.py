# -*- coding: utf-8 -*-
"""
Schedules for br-bcb-taxa-selic
"""

from datetime import datetime

from prefect.schedules import Schedule, adjustments, filters
from prefect.schedules.clocks import CronClock

from pipelines.constants import constants

schedule_every_weekday_taxa_selic = Schedule(
    clocks=[
        CronClock(
            cron="0 8 * * *",  # every day at 8 am
            start_date=datetime(2023, 6, 14, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_bcb_taxa_selic",
                "table_id": "taxa_selic",
                "materialization_mode": "prod",
                "materialize_after_dump": True,
                "update_metadata": True,
                "dbt_alias": False,
            },
        )
    ],
    filters=[filters.is_weekday],
    adjustments=[adjustments.next_weekday],
)
