# -*- coding: utf-8 -*-
"""
Schedules for br_ons_estimativa_custos
"""


from datetime import datetime

from prefect.schedules import Schedule, adjustments, filters
from prefect.schedules.clocks import CronClock

from pipelines.constants import constants

schedule_br_ons_avaliacao_operacao_reservatorio = Schedule(
    clocks=[
        CronClock(
            cron="@monthly",
            start_date=datetime(2023, 6, 14, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_ons_avaliacao_operacao",
                "table_id": "reservatorio",
                "materialization_mode": "prod",
                "materialize after dump": True,
                "update_metadata": True,
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
            cron="0 1 * * *",  # every day at midnight
            start_date=datetime(2023, 6, 14, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_ons_avaliacao_operacao",
                "table_id": "geracao_usina",
                "materialization_mode": "prod",
                "materialize_after_dump": True,
                "update_metadata": True,
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
            cron="0 2 * * *",  # every day at midnight
            start_date=datetime(2023, 6, 14, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_ons_avaliacao_operacao",
                "table_id": "geracao_termica_motivo_despacho",
                "materialization_mode": "prod",
                "materialize_after_dump": True,
                "update_metadata": True,
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
            cron="0 3 * * *",  # every day at midnight
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            start_date=datetime(2023, 6, 14, 0, 0),
            parameter_defaults={
                "dataset_id": "br_ons_avaliacao_operacao",
                "table_id": "energia_natural_afluente",
                "materialization_mode": "prod",
                "materialize_after_dump": True,
                "update_metadata": True,
                "dbt_alias": False,
            },
        )
    ],
    filters=[filters.is_weekday],
    adjustments=[adjustments.next_weekday],
)

schedule_br_ons_avaliacao_operacao_energia_armazenada_reservatorio = Schedule(
    clocks=[
        CronClock(
            cron="0 4 * * *",  # every day at midnight
            start_date=datetime(2023, 6, 14, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_ons_avaliacao_operacao",
                "table_id": "energia_armazenada_reservatorio",
                "materialization_mode": "prod",
                "materialize_after_dump": True,
                "update_metadata": True,
                "dbt_alias": False,
            },
        )
    ],
    filters=[filters.is_weekday],
    adjustments=[adjustments.next_weekday],
)


schedule_br_ons_avaliacao_operacao_restricao_operacao_usinas_eolicas = Schedule(
    clocks=[
        CronClock(
            cron="0 4 * * *",  # every day at midnight
            # todo: mudar horários
            start_date=datetime(2023, 6, 14, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_ons_avaliacao_operacao",
                "table_id": "restricao_operacao_usinas_eolicas",
                "materialization_mode": "prod",
                "materialize_after_dump": True,
                "update_metadata": True,
                "dbt_alias": False,
            },
        )
    ],
    filters=[filters.is_weekday],
    adjustments=[adjustments.next_weekday],
)
