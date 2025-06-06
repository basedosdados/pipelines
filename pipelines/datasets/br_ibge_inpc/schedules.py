# -*- coding: utf-8 -*-
"""
Schedules for br_ibge_inpc
    - mes_categoria_brasil
    - mes_categoria_rm
    - mes_categoria_municipio
    - mes_brasil
"""

from datetime import datetime

from prefect.schedules import Schedule, adjustments, filters
from prefect.schedules.clocks import CronClock

from pipelines.constants import constants

schedule_br_ibge_inpc_mes_categoria_brasil = Schedule(
    clocks=[
        CronClock(
            cron="50 14 * * *",  # everyday at 14:50:00
            start_date=datetime(2023, 10, 6, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "indice": "inpc",
                "folder": "br",
                "dataset_id": "br_ibge_inpc",
                "table_id": "mes_categoria_brasil",
                "target": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "update_metadata": True,
            },
        )
    ],
    filters=[filters.is_weekday],
    adjustments=[adjustments.next_weekday],
)

schedule_br_ibge_inpc_mes_categoria_rm = Schedule(
    clocks=[
        CronClock(
            cron="40 14 * * *",  # everyday at 14:40:00
            start_date=datetime(2023, 10, 6, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "indice": "inpc",
                "folder": "rm",
                "dataset_id": "br_ibge_inpc",
                "table_id": "mes_categoria_rm",
                "target": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "update_metadata": True,
            },
        )
    ],
    filters=[filters.is_weekday],
    adjustments=[adjustments.next_weekday],
)


schedule_br_ibge_inpc_mes_categoria_municipio = Schedule(
    clocks=[
        CronClock(
            cron="30 14 * * *",  # everyday at 14:30:00
            start_date=datetime(2023, 10, 6, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "indice": "inpc",
                "folder": "mun",
                "dataset_id": "br_ibge_inpc",
                "table_id": "mes_categoria_municipio",
                "target": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "update_metadata": True,
            },
        )
    ],
    filters=[filters.is_weekday],
    adjustments=[adjustments.next_weekday],
)


schedule_br_ibge_inpc_mes_brasil = Schedule(
    clocks=[
        CronClock(
            cron="20 14 * * *",  # everyday at 14:20:00
            start_date=datetime(2023, 10, 6, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "indice": "inpc",
                "folder": "mes",
                "dataset_id": "br_ibge_inpc",
                "table_id": "mes_brasil",
                "target": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "update_metadata": True,
            },
        )
    ],
    filters=[filters.is_weekday],
    adjustments=[adjustments.next_weekday],
)
