# -*- coding: utf-8 -*-
"""
Schedules for br_cvm_fi
"""

from datetime import timedelta, datetime
from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock
from pipelines.constants import constants
from prefect.schedules.clocks import CronClock
from pipelines.datasets.br_cvm_fi.constants import constants as cvm_constants


every_day_informe = Schedule(
    clocks=[
        CronClock(
            cron="0 17 * * *",  # At 17:00 on every day-of-week from Monday through Friday.
            start_date=datetime(2021, 3, 31, 17, 11),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_cvm_fi",
                "table_id": "documentos_informe_diario",
                "materialization_mode": "prod",
                "materialize_after_dump": True,
                "dbt_alias": False,
                "url": cvm_constants.INFORME_DIARIO_URL.value,
                "update_metadata": True,
            },
        ),
    ],
)

every_day_carteiras = Schedule(
    clocks=[
        CronClock(
            cron="20 17 * * *",  # At 13:20 on every day-of-week from Monday through Friday.
            start_date=datetime(2021, 3, 31, 17, 11),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_cvm_fi",
                "table_id": "documentos_carteiras_fundos_investimento",
                "materialization_mode": "prod",
                "materialize_after_dump": True,
                "dbt_alias": False,
                "url": cvm_constants.CDA_URL.value,
                "update_metadata": True,
            },
        ),
    ],
)

every_day_extratos = Schedule(
    clocks=[
        CronClock(
            cron="40 17 * * *",  # At 13:40 on every day-of-week from Monday through Friday.
            start_date=datetime(2021, 3, 31, 17, 11),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_cvm_fi",
                "table_id": "documentos_extratos_informacoes",
                "materialization_mode": "prod",
                "materialize_after_dump": True,
                "dbt_alias": False,
                "url": cvm_constants.URL_EXT.value,
                "file": cvm_constants.FILE_EXT.value,
                "update_metadata": True,
            },
        ),
    ],
)


every_day_perfil = Schedule(
    clocks=[
        CronClock(
            cron="0 18 * * *",  # At 14:00 on every day-of-week from Monday through Friday.
            start_date=datetime(2021, 3, 31, 17, 11),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_cvm_fi",
                "table_id": "documentos_perfil_mensal",
                "materialization_mode": "prod",
                "materialize_after_dump": True,
                "dbt_alias": False,
                "url": cvm_constants.URL_PERFIL_MENSAL.value,
                "update_metadata": True,
            },
        ),
    ],
)

every_day_informacao_cadastral = Schedule(
    clocks=[
        CronClock(
            cron="20 18 * * *",  # At 14:20 on every day-of-week from Monday through Friday.
            start_date=datetime(2021, 3, 31, 17, 11),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_cvm_fi",
                "table_id": "documentos_informacao_cadastral",
                "materialization_mode": "prod",
                "materialize_after_dump": True,
                "dbt_alias": False,
                "url": cvm_constants.URL_INFO_CADASTRAL.value,
                "files": cvm_constants.CAD_FILE.value,
                "update_metadata": True,
            },
        ),
    ],
)

every_day_balancete = Schedule(
    clocks=[
        CronClock(
            cron="40 18 * * *",  # At 14:40 on every day-of-week from Monday through Friday.
            start_date=datetime(2021, 3, 31, 17, 11),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_cvm_fi",
                "table_id": "documentos_balancete",
                "materialization_mode": "prod",
                "materialize_after_dump": True,
                "dbt_alias": False,
                "url": cvm_constants.URL_BALANCETE.value,
                "update_metadata": True,
            },
        ),
    ],
)
