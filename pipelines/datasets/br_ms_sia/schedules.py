# -*- coding: utf-8 -*-
"""
Schedules for br_ms_sia
"""

from datetime import datetime

from prefect.schedules import Schedule, adjustments, filters
from prefect.schedules.clocks import CronClock

from pipelines.constants import constants

schedule_br_ms_sia_producao_ambulatorial = Schedule(
    clocks=[
        CronClock(
            cron="0 7 * * *",  # every day at 7:00
            start_date=datetime(2024, 1, 1, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_ms_sia",
                "table_id": "producao_ambulatorial",
                "materialization_mode": "prod",
                "materialize_after_dump": True,
                "update_metadata": True,
                "dbt_alias": True,
            },
        )
    ],
    filters=[filters.is_weekday],
    adjustments=[adjustments.next_weekday],
)


schedule_br_ms_sia_psicossocial = Schedule(
    clocks=[
        CronClock(
            cron="0 7 * * *",  # every day at 7:00
            start_date=datetime(2024, 1, 1, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_ms_sia",
                "table_id": "psicossocial",
                "materialization_mode": "prod",
                "materialize_after_dump": True,
                "update_metadata": True,
                "dbt_alias": True,
            },
        )
    ],
    filters=[filters.is_weekday],
    adjustments=[adjustments.next_weekday],
)

