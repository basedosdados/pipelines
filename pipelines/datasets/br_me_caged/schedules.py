"""
Schedules for br_me_caged
"""

from datetime import datetime

from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock

from pipelines.constants import constants

every_month_movimentacao = Schedule(
    clocks=[
        CronClock(
            cron="0 8,17 25 * *",
            start_date=datetime(2025, 2, 26),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_me_caged",
                "table_id": "microdados_movimentacao",
                "target": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "update_metadata": True,
            },
        ),
    ]
)

every_month_movimentacao_fora_prazo = Schedule(
    clocks=[
        CronClock(
            cron="0 8,17 25 * *",
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            start_date=datetime(2025, 2, 26),
            parameter_defaults={
                "dataset_id": "br_me_caged",
                "table_id": "microdados_movimentacao_fora_prazo",
                "target": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "update_metadata": True,
            },
        ),
    ]
)

every_month_movimentacao_excluida = Schedule(
    clocks=[
        CronClock(
            cron="0 8,17 25 * *",
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            start_date=datetime(2025, 2, 26),
            parameter_defaults={
                "dataset_id": "br_me_caged",
                "table_id": "microdados_movimentacao_excluida",
                "target": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "update_metadata": True,
            },
        ),
    ]
)
