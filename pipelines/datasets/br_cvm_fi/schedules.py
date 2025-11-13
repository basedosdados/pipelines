"""
Schedules for br_cvm_fi
"""

from datetime import datetime

from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock

from pipelines.constants import constants

every_day_informe = Schedule(
    clocks=[
        CronClock(
            cron="0 17 * * *",  # At 17:00 on every day-of-week from Monday through Friday.
            start_date=datetime(2025, 10, 27, 17, 00),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_cvm_fi",
                "table_id": "documentos_informe_diario",
                "target": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "date_column_name": {"date": "data_competencia"},
                "update_metadata": True,
            },
        ),
    ],
)

every_day_carteiras = Schedule(
    clocks=[
        CronClock(
            cron="10 17 * * *",  # At 17:10 on every day-of-week from Monday through Friday.
            start_date=datetime(2025, 10, 27, 17, 10),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_cvm_fi",
                "table_id": "documentos_carteiras_fundos_investimento",
                "target": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "date_column_name": {"date": "data_competencia"},
                "update_metadata": True,
            },
        ),
    ],
)

every_day_extratos = Schedule(
    clocks=[
        CronClock(
            cron="20 17 * * *",  # At 17:20 on every day-of-week from Monday through Friday.
            start_date=datetime(2025, 10, 27, 17, 20),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_cvm_fi",
                "table_id": "documentos_extratos_informacoes",
                "target": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "date_column_name": {"date": "data_competencia"},
                "update_metadata": True,
            },
        ),
    ],
)


every_day_informacao_cadastral = Schedule(
    clocks=[
        CronClock(
            cron="40 17 * * *",  # At 17:40 on every day-of-week from Monday through Friday.
            start_date=datetime(2025, 10, 27, 17, 40),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_cvm_fi",
                "table_id": "documentos_informacao_cadastral",
                "target": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "date_column_name": {"date": "data_inicio_status"},
                "update_metadata": True,
            },
        ),
    ],
)

every_day_balancete = Schedule(
    clocks=[
        CronClock(
            cron="30 17 * * *",  # At 17:00 on every day-of-week from Monday through Friday.
            start_date=datetime(2025, 10, 27, 17, 30),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_cvm_fi",
                "table_id": "documentos_balancete",
                "target": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "date_column_name": {"date": "data_competencia"},
                "update_metadata": True,
            },
        ),
    ],
)


every_day_perfil = Schedule(
    clocks=[
        CronClock(
            cron="50 17 * * *",  # At 17:50 on every day-of-week from Monday through Friday.
            start_date=datetime(2025, 10, 27, 17, 50),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_cvm_fi",
                "table_id": "documentos_perfil_mensal",
                "target": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "date_column_name": {"date": "data_competencia"},
                "update_metadata": True,
            },
        ),
    ],
)
