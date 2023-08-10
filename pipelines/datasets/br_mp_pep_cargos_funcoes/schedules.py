# -*- coding: utf-8 -*-
"""
Schedules for br_mp_pep_cargos_funcoes
"""

from datetime import datetime
from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock
from pipelines.constants import constants

every_month = Schedule(
    clocks=[
        CronClock(
            cron="0 0 */10 * *",  # A cada 10 dias a meia-noite
            start_date=datetime(2023, 8, 2, 10, 30),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_mp_pep",
                "table_id": "cargos_funcoes",
                "materialization_mode": "prod",
                "materialize_after_dump": True,
                "dbt_alias": False,
            },
        ),
    ]
)
