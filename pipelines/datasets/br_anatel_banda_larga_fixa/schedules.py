# -*- coding: utf-8 -*-
"""
Schedules for dataset br_anatel_banda_larga_fixa
"""

from datetime import datetime

from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock, IntervalClock

from pipelines.constants import constants

every_month_anatel_microdados = Schedule(
    clocks=[
        CronClock(
            cron="0 15 * * *",  # ! goes execute every day 12 pm
            start_date=datetime(2023, 4, 3, 7, 5, 0),  # ! first date that the pipeline executed
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,  # ! label of identify (ex: basedosdados-dev)
            ],
            parameter_defaults={
                "dataset_id": "br_anatel_banda_larga_fixa",
                "table_id": [
                    "microdados",
                    "densidade_brasil",
                    "densidade_uf",
                    "densidade_municipio",
                ],
                "materialization_mode": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "update_metadata": True,
            },
        ),
    ]
)
