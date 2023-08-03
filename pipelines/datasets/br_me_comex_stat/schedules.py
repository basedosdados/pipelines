# -*- coding: utf-8 -*-

"""
Schedules for br_me_comex_stat
"""

from datetime import timedelta, datetime
from prefect.schedules import Schedule, filters, adjustments
from prefect.schedules.clocks import CronClock

from pipelines.constants import constants

schedule_municipio_exportacao = Schedule(
    clocks=[
        CronClock(
            cron="@monthly",
            start_date=datetime(2023, 8, 8, 0, 0),
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
        CronClock(
            cron="@monthly",
            start_date=datetime(2023, 8, 8, 0, 0),
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
        CronClock(
            cron="@monthly",
            start_date=datetime(2023, 8, 8, 0, 0),
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
        CronClock(
            cron="@monthly",
            start_date=datetime(2023, 8, 8, 0, 0),
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
