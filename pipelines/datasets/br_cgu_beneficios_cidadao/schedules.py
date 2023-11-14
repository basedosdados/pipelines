# -*- coding: utf-8 -*-
"""
Schedules for br_cgu_bolsa_familia
"""

from datetime import datetime

from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock, IntervalClock

from pipelines.constants import constants

every_day_novo_bolsa_familia = Schedule(
    clocks=[
        CronClock(
            cron="0 18 * * *",  # At 18:00
            start_date=datetime(2021, 3, 31, 17, 11),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_cgu_beneficios_cidadao",
                "table_id": "novo_bolsa_familia",
                "materialization_mode": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "historical_data": False,
                "update_metadata": True,
            },
        ),
    ],
)

every_day_garantia_safra = Schedule(
    clocks=[
        CronClock(
            cron="30 18 * * *",  # At 18:30
            start_date=datetime(2021, 3, 31, 17, 11),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_cgu_beneficios_cidadao",
                "table_id": "garantia_safra",
                "materialization_mode": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "historical_data": False,
                "update_metadata": True,
            },
        ),
    ],
)

every_day_bpc = Schedule(
    clocks=[
        CronClock(
            cron="00 19 * * *",  # At 19:00
            start_date=datetime(2021, 3, 31, 17, 11),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_cgu_beneficios_cidadao",
                "table_id": "bpc",
                "materialization_mode": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "historical_data": False,
                "update_metadata": True,
            },
        ),
    ],
)
