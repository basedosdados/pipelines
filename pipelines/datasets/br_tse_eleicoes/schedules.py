# -*- coding: utf-8 -*-
"""
Schedules for br_tse_eleicoes
"""

from datetime import datetime

from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock

from pipelines.constants import constants


every_monday_thursday = Schedule(
    clocks=[
        CronClock(
            cron="0 21 * * 1,4",  # 0 21 * * 1,4 means At 21:00 on Monday and Thursday.
            start_date=datetime(2021, 1, 1),
            labels=[
                constants.BASEDOSDADOS_DEV_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_tse_eleicoes",
                "materialization_mode": "dev",
                "materialize after dump": True,
                "table_id": "detalhes_votacao_secao",
                "dbt_alias": False,
            },
        )
    ],
)
