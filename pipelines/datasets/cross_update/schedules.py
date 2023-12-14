# -*- coding: utf-8 -*-
"""
Schedules for br_tse_eleicoes
"""

from datetime import datetime

from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock

from pipelines.constants import constants

schedule_nrows = Schedule(
    clocks=[
        CronClock(
            cron="0 8 * * 2",
            start_date=datetime(2023, 5, 1, 7, 30),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dump_to_gcs": True,
                "mode": "prod",
                "days": 7,
                "update_metadata_table": True,
            },
        ),
    ]
)
