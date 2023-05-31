# -*- coding: utf-8 -*-
"""
Schedules for br_ons_estimativa_custos
"""

from datetime import timedelta, datetime
from prefect.schedules import Schedule, filters, adjustments
from prefect.schedules.clocks import CronClock

from pipelines.constants import constants

schedule_br_ons_estimativa_custos_custo_marginal_operacao_semi_horario = Schedule(
    clocks=[
        CronClock(
            cron="0 0 * * *",  # every day at midnight
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_ons_estimativa_custos",
                "table_id": "custo_marginal_operacao_semi_horario",
                "materialization_mode": "prod",
                "materialize after dump": True,
                "dbt_alias": False,
            },
        )
    ],
    filters=[filters.is_weekday],
    adjustments=[adjustments.next_weekday],
)

schedule_br_ons_estimativa_custos_custo_marginal_operacao_semanal = Schedule(
    clocks=[
        CronClock(
            cron="0 0 * * 1",  # every monday at midnight
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_ons_estimativa_custos",
                "table_id": "custo_marginal_operacao_semanal",
                "materialization_mode": "prod",
                "materialize after dump": True,
                "dbt_alias": False,
            },
        )
    ],
    filters=[filters.is_weekday],
    adjustments=[adjustments.next_weekday],
)

schedule_br_ons_estimativa_custos_balanco_energia_subsistemas = Schedule(
    clocks=[
        CronClock(
            cron="0 0 * * *",  # every day at midnight
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_ons_estimativa_custos",
                "table_id": "balanco_energia_subsistemas",
                "materialization_mode": "prod",
                "materialize after dump": True,
                "dbt_alias": False,
            },
        )
    ],
    filters=[filters.is_weekday],
    adjustments=[adjustments.next_weekday],
)

schedule_br_ons_estimativa_custos_balanco_energia_subsistemas_dessem = Schedule(
    clocks=[
        CronClock(
            cron="0 0 * * *",  # every day at midnight
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_ons_estimativa_custos",
                "table_id": "balanco_energia_subsistemas_dessem",
                "materialization_mode": "prod",
                "materialize after dump": True,
                "dbt_alias": False,
            },
        )
    ],
    filters=[filters.is_weekday],
    adjustments=[adjustments.next_weekday],
)
