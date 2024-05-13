# -*- coding: utf-8 -*-
"""
Schedules for dataset br_anatel_telefonia_movel
"""
from datetime import datetime

from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock, IntervalClock
from pipelines.constants import constants

# ? ----------------------- > Microdados
schedule_br_anatel_telefonia_movel__microdados = Schedule(
    clocks=[
        CronClock(
            cron="30 16 * * *",
            start_date=datetime(2024, 1, 1, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_anatel_telefonia_movel",
                "table_id": "microdados",
                "materialization_mode": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "update_metadata": True,
                "anos": "2024",
                "semestre": "1",
                "update_metadata": True,
            },
        ),
    ],
)

# ? ----------------------- > Densidade Municipio

schedule_br_anatel_telefonia_movel__municipio = Schedule(
    clocks=[
        CronClock(
            cron="30 17 * * *",
            start_date=datetime(2024, 1, 1, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_anatel_telefonia_movel",
                "table_id": "densidade_municipio",
                "materialization_mode": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "update_metadata": True,
            },
        ),
    ],
)

# ? ----------------------- > Densidade UF

schedule_br_anatel_telefonia_movel__uf = Schedule(
    clocks=[
        CronClock(
            cron="30 18 * * *",
            start_date=datetime(2024, 1, 1, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_anatel_telefonia_movel",
                "table_id": "densidade_uf",
                "materialization_mode": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "update_metadata": True,
            },
        ),
    ],
)

# ? ----------------------- > Densidade Brasil

schedule_br_anatel_telefonia_movel__brasil = Schedule(
    clocks=[
        CronClock(
            cron="30 19 * * *",
            start_date=datetime(2024, 1, 1, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_anatel_telefonia_movel",
                "table_id": "densidade_brasil",
                "materialization_mode": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "update_metadata": True,
            },
        ),
    ],
)