# -*- coding: utf-8 -*-
"""
Schedules for mundo_transfermarkt_competicoes_internacionais
"""
###############################################################################

from datetime import datetime

from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock

from pipelines.constants import constants

every_two_weeks = Schedule(
    clocks=[
        CronClock(
            cron="0 9 * 2-10 2",
            start_date=datetime(2023, 5, 1, 7, 30),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "mundo_transfermarkt_competicoes_internacionais",
                "table_id": "champions_league",
                "materialization_mode": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
            },
        ),
    ]
)
