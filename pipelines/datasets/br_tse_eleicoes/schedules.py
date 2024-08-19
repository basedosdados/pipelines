# -*- coding: utf-8 -*-
"""
Schedules for br_tse_eleicoes
"""

from datetime import datetime, timedelta

from prefect.schedules import Schedule, filters
from prefect.schedules.clocks import IntervalClock

from pipelines.constants import constants


schedule_candidatos = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=1),
            start_date=datetime(2024, 1, 1, 10, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_tse_eleicoes",
                "table_id": "candidatos",
                "materialization_mode": "prod",
                "materialize after dump": True,
                "dbt_alias": True,
                "update_metadata": True
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
                "materialization_mode": "prod",
                "materialize after dump": True,
                "dbt_alias": True,
                "update_metadata": True
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
                "materialization_mode": "prod",
                "materialize after dump": True,
                "dbt_alias": True,
                "update_metadata": True
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
                "materialization_mode": "prod",
                "materialize after dump": True,
                "dbt_alias": True,
                "update_metadata": True
            },
        ),
    ],
    filters=[filters.is_weekday],
)
