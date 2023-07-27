# -*- coding: utf-8 -*- dataset
from datetime import timedelta, datetime
from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock
from pipelines.constants import constants

all_day_cotacoes = Schedule(
    clocks=[
        CronClock(
            cron="15 3 * * 2-6",  # ! Intervalo de tempo entre as execuções
            start_date=datetime(2023, 5, 16),  # ! Data de início da execução
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,  # ! Label para identificar o agente que irá executar a pipeline (ex: basedosdados-dev)
            ],
            parameter_defaults={
                "dataset_id": "br_b3_cotacoes",  # ! dataset_id do dataset que será executado
                "table_id": "cotacoes",  # ! table_id do dataset que será executado
                "materialization_mode": "prod",  # ! Aonde o dataset será materializado (dev, prod ou prod-staging)
                "materialize_after_dump": True,  # ! Se o dataset será materializado após o dump
                "dbt_alias": True,
            },
        ),
    ]
)