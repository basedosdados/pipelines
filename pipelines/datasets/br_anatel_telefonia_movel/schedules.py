# -*- coding: utf-8 -*-
"""
Schedules for dataset br_anatel_telefonia_movel
"""
from datetime import datetime

from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock, IntervalClock
from pipelines.constants import constants

# ? ----------------------- > Microdados
anatel_microdados = Schedule(
    clocks=[
        CronClock(
            cron="30 16 * * *",  # ! goes execute every day 13 pm
            start_date=datetime(2021, 3, 31, 17, 11), # ! first date that the pipeline executed
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value, # ! label of identify (ex: basedosdados-dev)
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

anatel_densidade_municipio = Schedule(
    clocks=[
        CronClock(
            cron="30 17 * * *",  # ! goes execute every day 13 pm
            start_date=datetime(2021, 3, 31, 17, 11), # ! first date that the pipeline executed
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value, # ! label of identify (ex: basedosdados-dev)
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

anatel_densidade_uf = Schedule(
    clocks=[
        CronClock(
            cron="30 18 * * *",  # ! goes execute every day 13 pm
            start_date=datetime(2021, 3, 31, 17, 11), # ! first date that the pipeline executed
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value, # ! label of identify (ex: basedosdados-dev)
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

anatel_densidade_brasil = Schedule(
    clocks=[
        CronClock(
            cron="30 19 * * *",  # ! goes execute every day 13 pm
            start_date=datetime(2021, 3, 31, 17, 11), # ! first date that the pipeline executed
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value, # ! label of identify (ex: basedosdados-dev)
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