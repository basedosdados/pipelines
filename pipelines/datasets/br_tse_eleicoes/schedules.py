"""
Schedules for br_tse_eleicoes
"""

from datetime import datetime

from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock

from pipelines.constants import constants

schedule_candidatos = Schedule(
    clocks=[
        CronClock(
            cron="0 2 * * *",  # everyday at 04:05
            start_date=datetime(2024, 8, 1, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_tse_eleicoes",
                "table_id": "candidatos",
                "target": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "update_metadata": True,
            },
        )
    ]
)

schedule_bens = Schedule(
    clocks=[
        CronClock(
            cron="30 2 * * *",  # everyday at 04:05
            start_date=datetime(2024, 8, 1, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_tse_eleicoes",
                "table_id": "bens_candidato",
                "target": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "update_metadata": True,
            },
        )
    ]
)

schedule_despesa = Schedule(
    clocks=[
        CronClock(
            cron="0 3 * * *",  # everyday at 04:05
            start_date=datetime(2024, 8, 1, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_tse_eleicoes",
                "table_id": "despesas_candidato",
                "target": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "update_metadata": True,
            },
        )
    ]
)


schedule_receita = Schedule(
    clocks=[
        CronClock(
            cron="30 3 * * *",  # everyday at 04:05
            start_date=datetime(2024, 8, 1, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_tse_eleicoes",
                "table_id": "receitas_candidato",
                "target": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "update_metadata": True,
            },
        )
    ]
)
