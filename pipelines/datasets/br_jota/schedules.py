# -*- coding: utf-8 -*-
"""
Schedules for br_jota
"""

from datetime import datetime

from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock

from pipelines.constants import constants


schedule_eleicao_perfil_candidato = Schedule(
    clocks=[
        CronClock(
            cron="0 2 * * *", #everyday at 04:05
            start_date=datetime(2024, 9, 1, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PERGUNTAS_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_jota",
                "table_id": "eleicao_perfil_candidato_2024",
                "materialization_mode": "prod",
                "dbt_alias": True,
            },
        ),
    ],
)

schedule_eleicao_prestacao_contas_candidato = Schedule(
    clocks=[
        CronClock(
            cron="0 2 * * *", #everyday at 04:05
            start_date=datetime(2024, 9, 1, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PERGUNTAS_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_jota",
                "table_id": "eleicao_prestacao_contas_candidato_2024",
                "materialization_mode": "prod",
                "dbt_alias": True,
            },
        ),
    ],
)

schedule_contas_candidato_origem = Schedule(
    clocks=[
        CronClock(
            cron="0 2 * * *", #everyday at 04:05
            start_date=datetime(2024, 9, 1, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PERGUNTAS_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_jota",
                "table_id": "eleicao_prestacao_contas_candidato_origem_2024",
                "materialization_mode": "prod",
                "dbt_alias": True,
            },
        ),
    ],
)

schedule_prestacao_contas_partido = Schedule(
    clocks=[
        CronClock(
            cron="0 2 * * *", #everyday at 04:05
            start_date=datetime(2024, 9, 1, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PERGUNTAS_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_jota",
                "table_id": "eleicao_prestacao_contas_partido_2024",
                "materialization_mode": "prod",
                "dbt_alias": True,
            },
        ),
    ],
)
