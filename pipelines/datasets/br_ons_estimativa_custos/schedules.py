# -*- coding: utf-8 -*-
"""
Schedules for br_ons_estimativa_custos
"""

from datetime import datetime

from prefect.schedules import Schedule, adjustments, filters
from prefect.schedules.clocks import CronClock

from pipelines.constants import constants

schedule_br_ons_estimativa_custos_custo_marginal_operacao_semi_horario = Schedule(
    clocks=[
        CronClock(
            cron="15 4 * * *",  # every day at 3h30 am
            start_date=datetime(2023, 10, 24, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_ons_estimativa_custos",
                "table_id": "custo_marginal_operacao_semi_horario",
                "materialization_mode": "prod",
                "materialize_after_dump": True,
                "update_metadata": True,
                "dbt_alias": True,
            },
        )
    ],
    filters=[filters.is_weekday],
    adjustments=[adjustments.next_weekday],
)

schedule_br_ons_estimativa_custos_custo_marginal_operacao_semanal = Schedule(
    clocks=[
        CronClock(
            cron="30 3 * * 1,5",  # every monday and friday at 3h30 am
            start_date=datetime(2023, 10, 24, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_ons_estimativa_custos",
                "table_id": "custo_marginal_operacao_semanal",
                "materialization_mode": "prod",
                "materialize_after_dump": True,
                "update_metadata": True,
                "dbt_alias": True,
            },
        )
    ],
    filters=[filters.is_weekday],
    adjustments=[adjustments.next_weekday],
)

schedule_br_ons_estimativa_custos_balanco_energia_subsistemas = Schedule(
    clocks=[
        CronClock(
            cron="30 1 * * *",  # every day at 1H30 am
            start_date=datetime(2023, 10, 24, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_ons_estimativa_custos",
                "table_id": "balanco_energia_subsistemas",
                "materialization_mode": "prod",
                "materialize_after_dump": True,
                "update_metadata": True,
                "dbt_alias": True,
            },
        )
    ],
    filters=[filters.is_weekday],
    adjustments=[adjustments.next_weekday],
)

schedule_br_ons_estimativa_custos_balanco_energia_subsistemas_dessem = Schedule(
    clocks=[
        CronClock(
            cron="45 0 * * *",  # every day at 0H45 am
            start_date=datetime(2023, 10, 24, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_ons_estimativa_custos",
                "table_id": "balanco_energia_subsistemas_dessem",
                "materialization_mode": "prod",
                "materialize_after_dump": True,
                "update_metadata": True,
                "dbt_alias": True,
            },
        )
    ],
    filters=[filters.is_weekday],
    adjustments=[adjustments.next_weekday],
)


schedule_br_ons_estimativa_custos_custo_variavel_unitario_usinas_termicas = Schedule(
    clocks=[
        CronClock(
            cron="30 0 * * *",  # every day at 00h30 am
            start_date=datetime(2023, 10, 24, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_ons_estimativa_custos",
                "table_id": "custo_variavel_unitario_usinas_termicas",
                "materialization_mode": "prod",
                "materialize_after_dump": True,
                "update_metadata": True,
                "dbt_alias": True,
            },
        )
    ],
    filters=[filters.is_weekday],
    adjustments=[adjustments.next_weekday],
)
