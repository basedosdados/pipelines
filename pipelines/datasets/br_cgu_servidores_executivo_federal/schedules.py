# -*- coding: utf-8 -*-
"""
Schedules for br_cgu_servidores_executivo_federal
"""

from datetime import datetime

from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock

from pipelines.constants import constants
from pipelines.datasets.br_cgu_servidores_executivo_federal.constants import (
    constants as cgu_constants,
)

every_month = Schedule(
    clocks=[
        CronClock(
            cron="0 6 * * 0-5",  # At 06:00 on every day-of-week from Sunday through Friday.
            start_date=datetime(2023, 9, 26),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_cgu_servidores_executivo_federal",
                "table_id": list(cgu_constants.TABLES.value.keys()),
                "materialization_mode": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "update_metadata": True,
            },
        ),
    ]
)
