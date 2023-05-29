# -*- coding: utf-8 -*-
"""
Schedules for br_ons_estimativa_custos
"""


from prefect.schedules import Schedule, filters, adjustments
from prefect.schedules.clocks import CronClock

from pipelines.constants import constants

schedule_br_ons_avaliacao_operacao_reservatorio = Schedule(
    clocks=[
        CronClock(
            cron="0 0 * * *",  # every day at midnight
            labels=[
                constants.BASEDOSDADOS_DEV_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_ons_avaliacao_operacao",
                "table_id": "reservatorio",
                "materialization_mode": "dev",
                "materialize after dump": True,
                "dbt_alias": False,
            },
        )
    ],
    filters=[filters.is_weekday],
    adjustments=[adjustments.next_weekday],
)

schedule_br_ons_avaliacao_operacao_geracao_usina = Schedule(
    clocks=[
        CronClock(
            cron="0 0 * * *",  # every day at midnight
            labels=[
                constants.BASEDOSDADOS_DEV_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_ons_avaliacao_operacao",
                "table_id": "geracao_usina",
                "materialization_mode": "dev",
                "materialize after dump": True,
                "dbt_alias": False,
            },
        )
    ],
    filters=[filters.is_weekday],
    adjustments=[adjustments.next_weekday],
)

schedule_br_ons_avaliacao_operacao_geracao_termica_motivo_despacho = Schedule(
    clocks=[
        CronClock(
            cron="0 0 * * *",  # every day at midnight
            labels=[
                constants.BASEDOSDADOS_DEV_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_ons_avaliacao_operacao",
                "table_id": "geracao_termica_motivo_despacho",
                "materialization_mode": "dev",
                "materialize after dump": True,
                "dbt_alias": False,
            },
        )
    ],
    filters=[filters.is_weekday],
    adjustments=[adjustments.next_weekday],
)

schedule_br_ons_avaliacao_operacao_energia_natural_afluente = Schedule(
    clocks=[
        CronClock(
            cron="0 0 * * *",  # every day at midnight
            labels=[
                constants.BASEDOSDADOS_DEV_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_ons_avaliacao_operacao",
                "table_id": "energia_natural_afluente",
                "materialization_mode": "dev",
                "materialize after dump": True,
                "dbt_alias": False,
            },
        )
    ],
    filters=[filters.is_weekday],
    adjustments=[adjustments.next_weekday],
)

schedule_br_ons_avaliacao_operacao_energia_armazenada_reservatorio_dia = Schedule(
    clocks=[
        CronClock(
            cron="0 0 * * *",  # every day at midnight
            labels=[
                constants.BASEDOSDADOS_DEV_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_ons_avaliacao_operacao",
                "table_id": "energia_armazenada_reservatorio_dia",
                "materialization_mode": "dev",
                "materialize after dump": True,
                "dbt_alias": False,
            },
        )
    ],
    filters=[filters.is_weekday],
    adjustments=[adjustments.next_weekday],
)
