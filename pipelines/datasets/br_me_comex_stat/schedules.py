# -*- coding: utf-8 -*-

"""
Schedules for br_me_comex_stat
"""

from datetime import timedelta, datetime

from prefect.schedules import Schedule, filters, adjustments
from prefect.schedules.clocks import IntervalClock

from pipelines.constants import constants

schedule_municipio_exportacao = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=30),
            start_date=datetime(1997, 1, 1, 14, 32),  # confirmar aqui
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_me_comex_stat",
                "table_id": "municipio_exportacao",
                "materialization_mode": "prod",
                "materialize after dump": True,
                "dbt_alias": False,
            },
        )
    ],
    filters=[filters.is_weekday],
    adjustments=[adjustments.next_weekday],
)


schedule_municipio_importacao = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=30),
            start_date=datetime(1997, 1, 1, 14, 32),  # confirmar aqui
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_me_comex_stat",
                "table_id": "municipio_importacao",
                "materialization_mode": "prod",
                "materialize after dump": True,
                "dbt_alias": False,
            },
        )
    ],
    filters=[filters.is_weekday],
    adjustments=[adjustments.next_weekday],
)

schedule_ncm_importacao = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=30),
            start_date=datetime(1997, 1, 1, 14, 32),  # confirmar aqui
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_me_comex_stat",
                "table_id": "ncm_importacao",
                "materialization_mode": "prod",
                "materialize after dump": True,
                "dbt_alias": False,
            },
        )
    ],
    filters=[filters.is_weekday],
    adjustments=[adjustments.next_weekday],
)

schedule_ncm_exportacao = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=30),
            start_date=datetime(1997, 1, 1, 14, 32),  # confirmar aqui
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_me_comex_stat",
                "table_id": "ncm_exportacao",
                "materialization_mode": "prod",
                "materialize after dump": True,
                "dbt_alias": False,
            },
        )
    ],
    filters=[filters.is_weekday],
    adjustments=[adjustments.next_weekday],
)
