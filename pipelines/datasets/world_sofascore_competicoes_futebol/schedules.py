# -*- coding: utf-8 -*-
"""
Schedules for br_tse_eleicoes
"""

from datetime import datetime

from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock

from pipelines.constants import constants

schedule_uefa_champions_league = Schedule(
    clocks=[
        CronClock(
            cron="0 2 * * *",  # everyday at 04:05
            start_date=datetime(2025, 2, 25, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "world_sofascore_competicoes_futebol",
                "table_id": "uefa_champions_league",
                "materialization_mode": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "update_metadata": True,
            },
        )
    ]
)

schedule_brasileirao_serie_a = Schedule(
    clocks=[
        CronClock(
            cron="30 2 * * *",  # everyday at 04:05
            start_date=datetime(2025, 2, 25, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "world_sofascore_competicoes_futebol",
                "table_id": "brasileirao_serie_a",
                "materialization_mode": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "update_metadata": True,
            },
        )
    ]
)
