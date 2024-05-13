# -*- coding: utf-8 -*-
"""
Schedules for dataset br_anatel_banda_larga_fixa
"""
from datetime import datetime

from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock, IntervalClock

from pipelines.constants import constants

schedule_br_anatel_banda_larga_fixa__microdados = Schedule(
    clocks=[
        CronClock(
            cron="0 15 * * *",
            start_date=datetime(2024, 1, 1, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_anatel_banda_larga_fixa",
                "table_id": "microdados",
                "materialization_mode": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "update_metadata": True,
            },
        ),
    ]
)

schedule_br_anatel_banda_larga_fixa__densidade_municipio = Schedule(
    clocks=[
        CronClock(
            cron="0 16 * * *",
            start_date=datetime(2024, 1, 1, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_anatel_banda_larga_fixa",
                "table_id": "densidade_municipio",
                "materialization_mode": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "update_metadata": True,
            },
        ),
    ]
)

schedule_br_anatel_banda_larga_fixa__densidade_brasil = Schedule(
    clocks=[
        CronClock(
            cron="0 17 * * *",
            start_date=datetime(2024, 1, 1, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_anatel_banda_larga_fixa",
                "table_id": "densidade_brasil",
                "materialization_mode": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "update_metadata": True,
            },
        ),
    ]
)

schedule_br_anatel_banda_larga_fixa__densidade_uf = Schedule(
    clocks=[
        CronClock(
            cron="0 18 * * *",
            start_date=datetime(2024, 1, 1, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_anatel_banda_larga_fixa",
                "table_id": "densidade_uf",
                "materialization_mode": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "update_metadata": True,
            },
        ),
    ]
)
