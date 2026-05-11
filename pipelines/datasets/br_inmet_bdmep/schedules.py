"""
Schedules for br_inmet_bdmep
"""

from datetime import datetime

from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock

from pipelines.constants import constants

d = datetime.today()
every_month_inmet = Schedule(
    clocks=[
        CronClock(
            cron="00 10,22 1-3 * *",
            start_date=datetime(2026, 4, 28, 8, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_inmet_bdmep",
                "table_id": "estacao",
                "target": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "update_metadata": True,
            },
        ),
    ]
)
