# -*- coding: utf-8 -*-
"""
Schedules for br_cgu_licitacao_contrato
"""

# -*- coding: utf-8 -*-
from datetime import datetime

from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock

from pipelines.constants import constants

every_day_contrato_compra = Schedule(
    clocks=[
        CronClock(
            cron="0 21 * * *",
            start_date=datetime(2021, 3, 31, 17, 11),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_cgu_licitacao_contrato",
                "table_id": "contrato_compra",
                "target": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "update_metadata": True,
            },
        ),
    ],
)

every_day_contrato_item = Schedule(
    clocks=[
        CronClock(
            cron="15 20 * * *",
            start_date=datetime(2021, 3, 31, 17, 11),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_cgu_licitacao_contrato",
                "table_id": "contrato_item",
                "target": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "update_metadata": True,
            },
        ),
    ],
)

every_day_contrato_termo_aditivo = Schedule(
    clocks=[
        CronClock(
            cron="30 20 * * *",
            start_date=datetime(2021, 3, 31, 17, 11),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_cgu_licitacao_contrato",
                "table_id": "contrato_termo_aditivo",
                "target": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "update_metadata": True,
            },
        ),
    ],
)

every_day_licitacao = Schedule(
    clocks=[
        CronClock(
            cron="45 20 * * *",
            start_date=datetime(2021, 3, 31, 17, 11),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_cgu_licitacao_contrato",
                "table_id": "licitacao",
                "target": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "update_metadata": True,
            },
        ),
    ],
)

every_day_licitacao_empenho = Schedule(
    clocks=[
        CronClock(
            cron="0 21 * * *",
            start_date=datetime(2021, 3, 31, 17, 11),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_cgu_licitacao_contrato",
                "table_id": "licitacao_empenho",
                "target": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "update_metadata": True,
            },
        ),
    ],
)

every_day_licitacao_item = Schedule(
    clocks=[
        CronClock(
            cron="15 20 * * *",
            start_date=datetime(2021, 3, 31, 17, 11),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_cgu_licitacao_contrato",
                "table_id": "licitacao_item",
                "target": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "update_metadata": True,
            },
        ),
    ],
)

every_day_licitacao_participante = Schedule(
    clocks=[
        CronClock(
            cron="30 20 * * *",
            start_date=datetime(2021, 3, 31, 17, 11),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_cgu_licitacao_contrato",
                "table_id": "licitacao_participante",
                "target": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "update_metadata": True,
            },
        ),
    ],
)
