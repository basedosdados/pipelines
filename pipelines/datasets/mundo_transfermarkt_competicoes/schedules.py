# -*- coding: utf-8 -*-
"""
Schedules for mundo_transfermarkt_competicoes
"""

###############################################################################

from datetime import datetime

from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock

from pipelines.constants import constants

every_day_brasileirao = Schedule(
    clocks=[
        CronClock(
            cron="0 23 * * 1-5",
            start_date=datetime(2023, 5, 1, 7, 30),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "mundo_transfermarkt_competicoes",
                "table_id": "brasileirao_serie_a",
                "materialization_mode": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
            },
        ),
    ]
)


every_day_copa = Schedule(
    clocks=[
        CronClock(
            cron="30 23 * * 1-5",
            start_date=datetime(2023, 5, 1, 7, 30),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "mundo_transfermarkt_competicoes",
                "table_id": "copa_brasil",
                "materialization_mode": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
            },
        ),
    ]
)
