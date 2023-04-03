# -*- coding: utf-8 -*-
"""
Schedules for br_anatel_banda_larga_fixa
"""

from datetime import timedelta, datetime
from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock
from pipelines.constants import constants

every_month_anatel = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=30), # ! Intervalo de tempo entre as execuções
            start_date=datetime(2023, 4, 3, 7, 0, 0), # ! Data de início da execução
            labels=[
                constants.BASEDOSDADOS_DEV_AGENT_LABEL.value, # ! Label para identificar o agente que irá executar a pipeline (ex: basedosdados-dev)
            ],
            parameter_defaults={
                "dataset_id": "br_anatel_banda_larga_fixa", # ! dataset_id do dataset que será executado
                "table_id": "microdados", # ! table_id do dataset que será executado
                "materialization_mode": "dev", # ! Aonde o dataset será materializado (dev, prod ou prod-staging)
                "materialize_after_dump": True, # ! Se o dataset será materializado após o dump
                "dbt_alias": True,
            },
        ),  
    ]
)
