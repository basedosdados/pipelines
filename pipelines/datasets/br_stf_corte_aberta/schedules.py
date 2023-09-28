# -*- coding: utf-8 -*-
"""
Schedules for br_stf_corte_aberta
"""

from datetime import datetime

from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock

from pipelines.constants import constants

every_day_stf = Schedule(
    clocks=[
        CronClock(
            cron="0 12 * * *",  # Irá rodar todos os dias meio dia
            start_date=datetime(2021, 1, 1),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_stf_corte_aberta",
                "table_id": "decisoes",
                "materialization_mode": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "update_metadata": True,
            },
        ),
    ]
)
