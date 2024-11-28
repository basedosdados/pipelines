# -*- coding: utf-8 -*-
"""
Schedules for br_cgu_licitacao_contrato
"""
# -*- coding: utf-8 -*-
from datetime import datetime
from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock, IntervalClock
from pipelines.constants import constants
from pipelines.utils.crawler_cgu.constants import constants as constants_cgu

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
                "materialization_mode": "prod",
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
                "materialization_mode": "prod",
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
                "materialization_mode": "prod",
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
                "materialization_mode": "prod",
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
                "materialization_mode": "prod",
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
                "materialization_mode": "prod",
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
                "materialization_mode": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "update_metadata": True,
            },
        ),
    ],
)
