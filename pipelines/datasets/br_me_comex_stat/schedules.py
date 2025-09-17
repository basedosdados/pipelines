# -*- coding: utf-8 -*-

"""
Schedules for br_me_comex_stat
"""

from datetime import datetime

from prefect.schedules import Schedule, adjustments, filters
from prefect.schedules.clocks import CronClock

from pipelines.constants import constants

schedule_municipio_exportacao = Schedule(
    clocks=[
        CronClock(
            cron="0 21 * * *",  # At 21:00 on every day-of-month
            start_date=datetime(2023, 11, 22, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_me_comex_stat",
                "table_id": "municipio_exportacao",
                "target": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "update_metadata": True,
            },
        )
    ],
    filters=[filters.is_weekday],
    adjustments=[adjustments.next_weekday],
)

schedule_municipio_importacao = Schedule(
    clocks=[
        CronClock(
            cron="0 20 * * *",  # At 20:00 on every day-of-month
            start_date=datetime(2023, 11, 22, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_me_comex_stat",
                "table_id": "municipio_importacao",
                "target": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "update_metadata": True,
            },
        )
    ],
    filters=[filters.is_weekday],
    adjustments=[adjustments.next_weekday],
)

schedule_ncm_importacao = Schedule(
    clocks=[
        CronClock(
            cron="0 8,17 * * *",  # At 8 and 17 on every day-of-month
            start_date=datetime(2023, 11, 22, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_me_comex_stat",
                "table_id": "ncm_importacao",
                "target": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "update_metadata": True,
            },
        )
    ],
    filters=[filters.is_weekday],
    adjustments=[adjustments.next_weekday],
)

schedule_ncm_exportacao = Schedule(
    clocks=[
        CronClock(
            cron="0 8,17 * * *",  # At 8 and 17 on every day-of-month
            start_date=datetime(2023, 11, 22, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_me_comex_stat",
                "table_id": "ncm_exportacao",
                "target": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "update_metadata": True,
            },
        )
    ],
    filters=[filters.is_weekday],
    adjustments=[adjustments.next_weekday],
)
