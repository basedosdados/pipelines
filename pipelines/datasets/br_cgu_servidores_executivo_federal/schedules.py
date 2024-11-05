# -*- coding: utf-8 -*-
"""
Schedules for br_cgu_servidores_executivo_federal
"""

from datetime import datetime

from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock
from pipelines.constants import constants

every_day_cadastro_aposentados = Schedule(
    clocks=[
        CronClock(
            cron="0 6 * * *",  # At 06:00 on every day-of-week from Sunday through Friday.
            start_date=datetime(2023, 9, 26),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_cgu_servidores_executivo_federal",
                "table_id": "cadastro_aposentados",
                "materialization_mode": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "update_metadata": True,
            },
        ),
    ]
)

every_day_cadastro_pensionistas = Schedule(
    clocks=[
        CronClock(
            cron="15 6 * * *",  # At 06:00 on every day-of-week from Sunday through Friday.
            start_date=datetime(2023, 9, 26),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_cgu_servidores_executivo_federal",
                "table_id": "cadastro_pensionistas",
                "materialization_mode": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "update_metadata": True,
            },
        ),
    ]
)

every_day_cadastro_servidores = Schedule(
    clocks=[
        CronClock(
            cron="30 6 * * *",  # At 06:00 on every day-of-week from Sunday through Friday.
            start_date=datetime(2023, 9, 26),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_cgu_servidores_executivo_federal",
                "table_id": "cadastro_servidores",
                "materialization_mode": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "update_metadata": True,
            },
        ),
    ]
)


every_day_cadastro_reserva_reforma_militares = Schedule(
    clocks=[
        CronClock(
            cron="45 6 * * *",  # At 06:00 on every day-of-week from Sunday through Friday.
            start_date=datetime(2023, 9, 26),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_cgu_servidores_executivo_federal",
                "table_id": "cadastro_reserva_reforma_militares",
                "materialization_mode": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "update_metadata": True,
            },
        ),
    ]
)


every_day_remuneracao = Schedule(
    clocks=[
        CronClock(
            cron="0 7 * * *",  # At 06:00 on every day-of-week from Sunday through Friday.
            start_date=datetime(2023, 9, 26),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_cgu_servidores_executivo_federal",
                "table_id": "remuneracao",
                "materialization_mode": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "update_metadata": True,
            },
        ),
    ]
)

every_day_afastamentos = Schedule(
    clocks=[
        CronClock(
            cron="15 7 * * *",  # At 06:00 on every day-of-week from Sunday through Friday.
            start_date=datetime(2023, 9, 26),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_cgu_servidores_executivo_federal",
                "table_id": "afastamentos",
                "materialization_mode": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "update_metadata": True,
            },
        ),
    ]
)

every_day_observacoes = Schedule(
    clocks=[
        CronClock(
            cron="30 7 * * *",  # At 06:00 on every day-of-week from Sunday through Friday.
            start_date=datetime(2023, 9, 26),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_cgu_servidores_executivo_federal",
                "table_id": "observacoes",
                "materialization_mode": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "update_metadata": True,
            },
        ),
    ]
)