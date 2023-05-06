# -*- coding: utf-8 -*-
"""
Schedules for br_cvm_fii
"""

from datetime import timedelta, datetime
from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock
from pipelines.constants import constants
from prefect.schedules.clocks import CronClock


every_day_cvm = Schedule(
    clocks=[
        CronClock(
            cron="0 13 * * 1-5",  # At 13:00 on every day-of-week from Monday through Friday.
            start_date=datetime(2021, 3, 31, 17, 11),
            labels=[
                constants.BASEDOSDADOS_DEV_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_cvm_fii",
                "table_id": "documentos_informe_diario",
                "materialization_mode": "dev",
                "materialize_after_dump": False,
                "dbt_alias": False,
            },
        ),
    ],
)
