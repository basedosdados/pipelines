# -*- coding: utf-8 -*-
"""
Schedules for dataset br_anatel_telefonia_movel
"""
from datetime import datetime

from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock, IntervalClock

from pipelines.constants import constants

every_month_anatel = Schedule(
    clocks=[
        CronClock(
            cron="0 16 * * *",  # 5h UTC = 2h BRT
            start_date=datetime(2021, 3, 31, 17, 11),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_anatel_telefonia_movel",
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
                "anos": "2023",
                "mes_um": "07",
                "mes_dois": "12",
                "update_metadata": True,
            },
        ),
    ],
)
