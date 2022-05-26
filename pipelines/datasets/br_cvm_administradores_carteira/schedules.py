# -*- coding: utf-8 -*-
"""
Schedules for br_cvm_administradores_carteira
"""
from datetime import timedelta, datetime
from prefect.schedules import Schedule, filters
from prefect.schedules.clocks import IntervalClock
from pipelines.constants import constants

schedule_responsavel = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=1),
            start_date=datetime(2021, 1, 1, 6, 12),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_cvm_administradores_carteira",
                "materialization_mode": "prod",
                "materialize after dump": True,
                "table_id": "responsavel",
            },
        )
    ],
    filters=[filters.is_weekday],
)

schedule_fisica = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=1),
            start_date=datetime(2021, 1, 1, 6,50),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_cvm_administradores_carteira",
                "materialization_mode": "prod",
                "materialize after dump": True,
                "table_id": "pessoa_fisica",
            },
        )
    ],
    filters=[filters.is_weekday],
)

schedule_juridica = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=1),
            start_date=datetime(2021, 1, 1, 6, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_cvm_administradores_carteira",
                "materialization_mode": "prod",
                "materialize after dump": True,
                "table_id": "pessoa_juridica",
            },
        )
    ],
    filters=[filters.is_weekday],
)
