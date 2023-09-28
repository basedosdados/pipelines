# -*- coding: utf-8 -*-
"""
Schedules for br_jota
"""

from datetime import datetime, timedelta

from prefect.schedules import Schedule, filters
from prefect.schedules.clocks import IntervalClock

from pipelines.constants import constants

schedule_candidatos = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=1),
            start_date=datetime(2021, 1, 1, 10, 45),
            labels=[
                constants.BASEDOSDADOS_PERGUNTAS_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_jota",
                "table_id": "eleicao_perfil_candidato_2022",
                "materialization_mode": "dev",
                "dbt_alias": False,
            },
        ),
    ],
    filters=[filters.is_weekday],
)

schedule_contas_candidato = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=1),
            start_date=datetime(2021, 1, 1, 10, 50),
            labels=[
                constants.BASEDOSDADOS_PERGUNTAS_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_jota",
                "table_id": "eleicao_prestacao_contas_candidato_2022",
                "materialization_mode": "dev",
                "dbt_alias": False,
            },
        ),
    ],
    filters=[filters.is_weekday],
)

schedule_contas_candidato_origem = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=1),
            start_date=datetime(2021, 1, 1, 10, 55),
            labels=[
                constants.BASEDOSDADOS_PERGUNTAS_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_jota",
                "table_id": "eleicao_prestacao_contas_candidato_origem_2022",
                "materialization_mode": "dev",
                "dbt_alias": False,
            },
        ),
    ],
    filters=[filters.is_weekday],
)
