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
            cron="0 15 * * *",  # ! goes execute every day 6 am
            start_date=datetime(2023, 4, 3, 7, 5, 0),  # ! Data de início da execução
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,  # ! Label para identificar o agente que irá executar a pipeline (ex: basedosdados-dev)
            ],
            parameter_defaults={
                "dataset_id": "br_anatel_banda_larga_fixa",  # ! dataset_id do dataset que será executado
                "table_id": [
                    "microdados",
                    "densidade_brasil",
                    "densidade_uf",
                    "densidade_municipio",
                ],  # ! table_id do dataset que será executado
                "materialization_mode": "prod",  # ! Aonde o dataset será materializado (dev, prod ou prod-staging)
                "materialize_after_dump": True,  # ! Se o dataset será materializado após o dump
                "dbt_alias": True,
                "update_metadata": True,
            },
        ),
    ]
)
