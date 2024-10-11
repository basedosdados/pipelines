# -*- coding: utf-8 -*-
"""
Schedules for br_sfb_sicar
"""

from datetime import datetime

from prefect.schedules import Schedule, adjustments, filters
from prefect.schedules.clocks import CronClock

from pipelines.constants import constants

schedule_br_sfb_sicar_area_imovel = Schedule(
    clocks=[
        CronClock(
            cron="15 21 15 * *", #At 21:15 on day-of-month 15
            start_date=datetime(2024, 10, 1, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_sfb_sicar",
                "table_id": "area_imovel",
                "materialization_mode": "prod",
                "materialize_after_dump": True,
                "update_metadata": True,
                "dbt_alias": True,
            },
        )
    ],
    filters=[filters.is_weekday],
    adjustments=[adjustments.next_weekday],
)
