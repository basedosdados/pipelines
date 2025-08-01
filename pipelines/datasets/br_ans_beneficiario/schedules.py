# -*- coding: utf-8 -*-
"""
Schedules for br_ans_beneficiario
"""

from datetime import datetime

from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock

from pipelines.constants import constants

every_day_ans = Schedule(
    clocks=[
        CronClock(
            cron="0 21 * * *",  # At 23:00 on every day-of-week from Monday through Friday.
            start_date=datetime(2021, 3, 31, 17, 11),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_ans_beneficiario",
                "table_id": "informacao_consolidada",
                "target": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "update_metadata": True,
            },
        ),
    ],
)
