# -*- coding: utf-8 -*-
"""
Schedules for br_tse_eleicoes
"""

from datetime import timedelta, datetime
from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock
from pipelines.constants import constants

schedule_nrows = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=1),
            start_date=datetime(2021, 1, 1, 9, 45),
            labels=[
                constants.BASEDOSDADOS_DEV_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_tse_eleicoes",
                "table_id": "candidatos",
                "start": 2018,
                "id_candidato_bd": False,
                "materialization_mode": "prod",
                "materialize after dump": True,
                "dbt_alias": False,
            },
        ),
    ],
)
