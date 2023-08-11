# -*- coding: utf-8 -*-
"""
Schedules for br_me_cnpj
"""

###############################################################################


from prefect.schedules.clocks import CronClock
from datetime import timedelta, datetime
from prefect.schedules import Schedule
from pipelines.constants import constants

every_ten_days_empresas = Schedule(
    clocks=[
        CronClock(
            cron="0 8 */10 * *",
            start_date=datetime(2023, 1, 1, 7, 30),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_me_cnpj",
                "table_id": "empresas",
                "materialization_mode": "prod",
                "materialize_after_dump": True,
                "dbt_alias": False,
            },
        ),
    ]
)
every_ten_days_socios = Schedule(
    clocks=[
        CronClock(
            cron="0 8 */10 * *",
            start_date=datetime(2023, 1, 1, 8, 30),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_me_cnpj",
                "table_id": "socios",
                "materialization_mode": "prod",
                "materialize_after_dump": True,
                "dbt_alias": False,
            },
        ),
    ]
)
every_ten_days_estabelecimentos = Schedule(
    clocks=[
        CronClock(
            cron="0 8 */10 * *",
            start_date=datetime(2023, 1, 1, 7, 30),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_me_cnpj",
                "table_id": "estabelecimentos",
                "materialization_mode": "prod",
                "materialize_after_dump": True,
                "dbt_alias": False,
            },
        ),
    ]
)
every_ten_days_simples = Schedule(
    clocks=[
        CronClock(
            cron="0 8 */10 * *",
            start_date=datetime(2023, 1, 1, 7, 30),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_me_simples",
                "table_id": "estabelecimentos",
                "materialization_mode": "prod",
                "materialize_after_dump": True,
                "dbt_alias": False,
            },
        ),
    ]
)
