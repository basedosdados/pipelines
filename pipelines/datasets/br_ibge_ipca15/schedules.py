# -*- coding: utf-8 -*-
"""
Schedules for br_ibge_ipca15
    - mes_categoria_brasil
    - mes_categoria_rm
    - mes_categoria_municipio
    - mes_brasil
"""
from datetime import datetime

from prefect.schedules import Schedule, adjustments, filters
from prefect.schedules.clocks import CronClock

from pipelines.constants import constants

schedule_br_ibge_ipca15_mes_categoria_brasil = Schedule(
    clocks=[
        CronClock(
            cron="30 13 * * *",  # everyday at 13:30:00
            start_date=datetime(2023, 10, 6, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "indice": "ip15",
                "folder": "br",
                "dataset_id": "br_ibge_ipca15",
                "table_id": "mes_categoria_brasil",
                "materialization_mode": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "update_metadata": True,
            },
        )
    ],
    filters=[filters.is_weekday],
    adjustments=[adjustments.next_weekday],
)

schedule_br_ibge_ipca15_mes_categoria_rm = Schedule(
    clocks=[
        CronClock(
            cron="20 13 * * *",  # everyday at 13:20:00
            start_date=datetime(2023, 10, 6, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "indice": "ip15",
                "folder": "rm",
                "dataset_id": "br_ibge_ipca15",
                "table_id": "mes_categoria_rm",
                "materialization_mode": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "update_metadata": True,
            },
        )
    ],
    filters=[filters.is_weekday],
    adjustments=[adjustments.next_weekday],
)


schedule_br_ibge_ipca15_mes_categoria_municipio = Schedule(
    clocks=[
        CronClock(
            cron="10 13 * * *",  # everyday at 13:10:00
            start_date=datetime(2023, 10, 6, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "indice": "ip15",
                "folder": "mun",
                "dataset_id": "br_ibge_ipca15",
                "table_id": "mes_categoria_municipio",
                "materialization_mode": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "update_metadata": True,
            },
        )
    ],
    filters=[filters.is_weekday],
    adjustments=[adjustments.next_weekday],
)


schedule_br_ibge_ipca15_mes_brasil = Schedule(
    clocks=[
        CronClock(
            cron="15 13 * * *",  # everyday at 13:00:00
            start_date=datetime(2023, 10, 6, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "indice": "ip15",
                "folder": "mes",
                "dataset_id": "br_ibge_ipca15",
                "table_id": "mes_brasil",
                "materialization_mode": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "update_metadata": True,
            },
        )
    ],
    filters=[filters.is_weekday],
    adjustments=[adjustments.next_weekday],
)
