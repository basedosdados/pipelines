# -*- coding: utf-8 -*-
"""
Schedules for br_rf_cafir
"""

from prefect.schedules import Schedule, filters, adjustments
from prefect.schedules.clocks import CronClock
from datetime import datetime
from pipelines.constants import constants


schedule_br_rf_cafir_imoveis_rurais = Schedule(
    clocks=[
        CronClock(
            cron="0 0 * * *",  # every day at midnight
            start_date=datetime(2023, 9, 1, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_rf_cafir",
                "table_id": "imoveis_rurais",
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
