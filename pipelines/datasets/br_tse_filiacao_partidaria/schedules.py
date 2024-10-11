# -*- coding: utf-8 -*-
"""
Schedules for br_tse_filiacao_partidaria
"""

from datetime import datetime

from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock

from pipelines.constants import constants


schedule_microdados = Schedule(
    clocks=[
        CronClock(
            cron="30 8 30 * *", #everymonth at 08:30
            start_date=datetime(2024, 10, 1, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_tse_filiacao_partidaria",
                "table_id": "microdados",
                "materialization_mode": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "update_metadata": True
            }
        )
    ]

)
