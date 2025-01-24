# -*- coding: utf-8 -*-
from datetime import datetime

from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock

from pipelines.constants import constants

every_day_emendas_parlamentares = Schedule(
    clocks=[
        CronClock(
            cron="30 19 * * *",  # At 19:30 every day
            start_date=datetime(2021, 3, 31, 17, 11),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_cgu_emendas_parlamentares",
                "table_id": "microdados",
                "materialization_mode": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "update_metadata": True,
            },
        ),
    ],
)
