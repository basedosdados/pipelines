# -*- coding: utf-8 -*-
"""
Schedules for br_inmet_bdmep
"""


from datetime import datetime

from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock, IntervalClock

from pipelines.constants import constants

d = datetime.today()
every_month_inmet = Schedule(
    clocks=[
        CronClock(
            cron="00 22 * * 1-5",  # At 22:00 on every day-of-week from Monday through Friday.
            start_date=datetime(2021, 3, 31, 17, 11),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_inmet_bdmep",
                "table_id": "microdados",
                "materialization_mode": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "year": d.strftime("%Y"),
                "update_metadata": True,
            },
        ),
    ],
)
