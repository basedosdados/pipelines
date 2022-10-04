# -*- coding: utf-8 -*-
"""
Schedules for br_ibge_pnadc
"""
from datetime import datetime

from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock

from pipelines.constants import constants

current_year = datetime.now().year
current_quarter = (datetime.now().month - 1) // 3 + 1
last_quarter = current_quarter - 1

every_quarter = Schedule(
    clocks=[
        CronClock(
            cron="00 15 15 5,8,11,2 *",  # At 15:00 on day-of-month 15 in May, August, November, and February
            start_date=datetime(2021, 1, 1, 15, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_ibge_pnadc",
                "table_id": "microdados",
                "year": current_year,
                "quarter": last_quarter,
                "materialization_mode": "prod",
                "materialize after dump": False,
                "dbt_alias": True,
            },
        )
    ],
)
