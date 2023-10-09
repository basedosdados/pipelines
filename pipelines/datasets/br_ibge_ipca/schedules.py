# -*- coding: utf-8 -*-
"""
Schedules for br_ibge_ipca
"""
from datetime import datetime

from prefect.schedules import Schedule, adjustments, filters
from prefect.schedules.clocks import CronClock

from pipelines.constants import constants

schedule_br_ibge_ipca_mes_categoria_brasil = Schedule(
    clocks=[
        CronClock(
            cron="30 14 * * *",  # everyday at 14:30:00
            start_date=datetime(2023, 10, 6, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "indice": "ipca",
                "folder": "br",
                "dataset_id": "br_ibge_ipca",
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

schedule_br_ibge_ipca_mes_categoria_rm = Schedule(
    clocks=[
        CronClock(
            cron="20 14 * * *",  # everyday at 14:20:00
            start_date=datetime(2023, 10, 6, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "indice": "ipca",
                "folder": "rm",
                "dataset_id": "br_ibge_ipca",
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


schedule_br_ibge_ipca_mes_categoria_municipio = Schedule(
    clocks=[
        CronClock(
            cron="50 13 * * *",  # everyday at 13:50:00
            start_date=datetime(2023, 10, 6, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "indice": "ipca",
                "folder": "mun",
                "dataset_id": "br_ibge_ipca",
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


schedule_br_ibge_ipca_mes_brasil = Schedule(
    clocks=[
        CronClock(
            cron="40 13 * * *",  # everyday at 13:40:00
            start_date=datetime(2023, 10, 6, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "indice": "ipca",
                "folder": "mes",
                "dataset_id": "br_ibge_ipca",
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
