"""
Schedules for br_bndes_operacoes_contratadas
"""

from datetime import datetime

from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock

from pipelines.constants import constants

every_week_bndes = Schedule(
    clocks=[
        CronClock(
            cron="00 6,18 * * 1",
            start_date=datetime(2026, 5, 18, 6, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_bndes_operacoes_contratadas",
                "target": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "update_metadata": True,
            },
        ),
    ]
)
