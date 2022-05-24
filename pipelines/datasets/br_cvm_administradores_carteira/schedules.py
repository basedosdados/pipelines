# -*- coding: utf-8 -*-
"""
Schedules for br_cvm_administradores_carteira
"""
from datetime import timedelta, datetime
from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock
from pipelines.constants import constants

schedule_responsavel = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=1),
            start_date=datetime(2021, 1, 1),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_cvm_administradores_carteira",
                "materialization_mode": "dev",
                "materialize after dump": True,
                "table_id": "responsavel",
            },
        )
    ],
)

schedule_fisica = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=1),
            start_date=datetime(2021, 1, 1),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_cvm_administradores_carteira",
                "materialization_mode": "dev",
                "materialize after dump": True,
                "table_id": "pessoa_fisica",
            },
        )
    ],
)

schedule_juridica = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=1),
            start_date=datetime(2021, 1, 1),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_cvm_administradores_carteira",
                "materialization_mode": "dev",
                "materialize after dump": True,
                "table_id": "pessoa_juridica",
            },
        )
    ],
)
