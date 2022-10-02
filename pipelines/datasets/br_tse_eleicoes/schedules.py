# -*- coding: utf-8 -*-
"""
Schedules for br_tse_eleicoes
"""

from datetime import timedelta, datetime

from prefect.schedules import Schedule, filters
from prefect.schedules.clocks import IntervalClock

from pipelines.constants import constants

schedule_candidatos = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=1),
            start_date=datetime(2021, 1, 1, 9, 45),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_tse_eleicoes",
                "table_id": "candidatos",
                "start": 2018,
                "id_candidato_bd": False,
                "materialization_mode": "prod",
                "materialize after dump": True,
                "dbt_alias": False,
            },
        ),
    ],
    filters=[filters.is_weekday],
)

schedule_bens = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=1),
            start_date=datetime(2021, 1, 1, 10, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_tse_eleicoes",
                "table_id": "bens_candidato",
                "start": 2018,
                "id_candidato_bd": False,
                "materialization_mode": "prod",
                "materialize after dump": True,
                "dbt_alias": False,
            },
        ),
    ],
    filters=[filters.is_weekday],
)

schedule_despesa = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=1),
            start_date=datetime(2021, 1, 1, 10, 15),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_tse_eleicoes",
                "table_id": "despesas_candidato",
                "id_candidato_bd": False,
                "materialization_mode": "prod",
                "materialize after dump": True,
                "dbt_alias": False,
            },
        ),
    ],
    filters=[filters.is_weekday],
)


schedule_receita = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=1),
            start_date=datetime(2021, 1, 1, 10, 30),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_tse_eleicoes",
                "table_id": "receitas_candidato",
                "id_candidato_bd": False,
                "materialization_mode": "prod",
                "materialize after dump": True,
                "dbt_alias": False,
            },
        ),
    ],
    filters=[filters.is_weekday],
)


schedule_apuracao = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(minutes=1),
            start_date=datetime(2021, 1, 1, 10, 30),
            labels=[
                constants.BASEDOSDADOS_DEV_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_tse_eleicoes",
                "table_id": "apuracao",
                "id_candidato_bd": False,
                "materialization_mode": "dev",
                "materialize after dump": False,
                "dbt_alias": False,
            },
        ),
    ]
)
