# -*- coding: utf-8 -*-
"""
Schedules for mundo_transfermarkt_competicoes
"""

###############################################################################

from prefect.schedules.clocks import CronClock
from datetime import timedelta, datetime
from prefect.schedules import Schedule
from pipelines.constants import constants


every_week = Schedule(
    clocks=[
        CronClock(
            cron="0 8 * 5-12 2",
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


every_week_copa = Schedule(
    clocks=[
        CronClock(
            cron="0 9 * 2-12 2",
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
