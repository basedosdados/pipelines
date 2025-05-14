# -*- coding: utf-8 -*-
"""
Schedules for br_me_cnpj
"""

from datetime import datetime

from prefect.schedules import Schedule

###############################################################################
from prefect.schedules.clocks import CronClock

from pipelines.constants import constants

every_day_empresas = Schedule(
    clocks=[
        CronClock(
            cron="0 6 * * *",
            start_date=datetime(2023, 1, 1, 7, 30),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_me_cnpj",
                "table_id": "empresas",
                "target": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
            },
        ),
    ]
)
every_day_socios = Schedule(
    clocks=[
        CronClock(
            cron="0 7 * * *",
            start_date=datetime(2023, 1, 1, 8, 30),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_me_cnpj",
                "table_id": "socios",
                "target": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
            },
        ),
    ]
)
every_day_simples = Schedule(
    clocks=[
        CronClock(
            cron="0 8 * * *",
            start_date=datetime(2023, 1, 1, 7, 30),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_me_cnpj",
                "table_id": "simples",
                "target": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
            },
        ),
    ]
)
every_day_estabelecimentos = Schedule(
    clocks=[
        CronClock(
            cron="0 9 * * *",
            start_date=datetime(2023, 1, 1, 7, 30),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_me_cnpj",
                "table_id": "estabelecimentos",
                "target": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
            },
        ),
    ]
)
