# -*- coding: utf-8 -*-
"""
Schedules for br_cgu_bolsa_familia
"""

from datetime import datetime

from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock

from pipelines.constants import constants

every_day_bolsa_familia = Schedule(
    clocks=[
        CronClock(
            cron="0 19 * * *",
            start_date=datetime(2021, 3, 31, 17, 11),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_cgu_beneficios_cidadao",
                "table_id": "novo_bolsa_familia",
                "target": "prod",
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
            cron="15 19 * * *",  # At 18:30
            start_date=datetime(2021, 3, 31, 17, 11),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_cgu_beneficios_cidadao",
                "table_id": "garantia_safra",
                "target": "prod",
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
            cron="30 19 * * *",  # At 19:00
            start_date=datetime(2021, 3, 31, 17, 11),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_cgu_beneficios_cidadao",
                "table_id": "bpc",
                "target": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "historical_data": False,
                "update_metadata": True,
            },
        ),
    ],
)
