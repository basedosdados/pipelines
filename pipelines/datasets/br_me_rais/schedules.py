"""
Schedules for br_me_rais
"""

from datetime import datetime

from prefect.schedules import Schedule, adjustments, filters
from prefect.schedules.clocks import CronClock

from pipelines.constants import constants

# Run on the first 7 days of February at 9 AM — when annual RAIS complete data is available
_cron = "0 9 1-7 2 *"
_start = datetime(2026, 2, 1)

every_year_estabelecimentos = Schedule(
    clocks=[
        CronClock(
            cron=_cron,
            start_date=_start,
            labels=[constants.BASEDOSDADOS_PROD_AGENT_LABEL.value],
            parameter_defaults={
                "dataset_id": "br_me_rais",
                "table_id": "microdados_estabelecimentos",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "update_metadata": True,
            },
        ),
    ],
    filters=[filters.is_weekday],
    adjustments=[adjustments.next_weekday],
)

every_year_vinculos = Schedule(
    clocks=[
        CronClock(
            cron=_cron,
            start_date=_start,
            labels=[constants.BASEDOSDADOS_PROD_AGENT_LABEL.value],
            parameter_defaults={
                "dataset_id": "br_me_rais",
                "table_id": "microdados_vinculos",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "update_metadata": True,
            },
        ),
    ],
    filters=[filters.is_weekday],
    adjustments=[adjustments.next_weekday],
)
