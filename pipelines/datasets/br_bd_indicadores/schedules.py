# -*- coding: utf-8 -*-
"""
Schedules for bd_tweet_data
"""

from datetime import timedelta, datetime

from prefect.schedules import Schedule, filters, adjustments
from prefect.schedules.clocks import IntervalClock

from pipelines.constants import constants

schedule_users = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=1),
            start_date=datetime(2022, 5, 15, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_bd_indicadores",
                "table_id": "website_user",
                "materialization_mode": "prod",
                "materialize after dump": True,
                "dbt_alias": False,
            },
        ),
    ],
    filters=[filters.is_weekday],
)

every_day = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=1),
            start_date=datetime(2022, 5, 18, 16, 24),
            labels=[
                constants.BASEDOSDADOS_DEV_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_bd_indicadores",
                "table_id": "twitter_metrics",
                "materialization_mode": "dev",
                "materialize after dump": True,
                "dbt_alias": False,
            },
        ),
    ],
    filters=[filters.is_weekday],
)

every_week = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(weeks=1),
            start_date=datetime(2021, 1, 1, 17, 35),
            labels=[
                constants.BASEDOSDADOS_DEV_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_bd_indicadores",
                "materialization_mode": "dev",
                "materialize after dump": True,
                "table_id": "twitter_metrics_agg",
            },
        ),
    ],
    filters=[filters.is_weekday],
    adjustments=[adjustments.next_weekday],
)

schedule_contabilidade = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=1),
            start_date=datetime(2022, 1, 1, 6, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_bd_indicadores",
                "table_id": "contabilidade",
                "sheet_id": "1jtZAV2SFEdEX99DumpUQ1LjZE2vcSgvL4DNo4n6HIec",
                "sheet_name": "transacoes_anonimizado",
                "materialization_mode": "prod",
                "materialize after dump": True,
            },
        ),
    ],
    filters=[filters.is_weekday],
    adjustments=[adjustments.next_weekday],
)

schedule_receitas = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=1),
            start_date=datetime(2022, 1, 1, 6, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_bd_indicadores",
                "table_id": "receitas_planejadas",
                "sheet_id": "1fHp1NNUyhFIAAJ9bZOdZ2i9PSLIbkjSjMcGAlaxur90",
                "sheet_name": "receitas_planejadas_anonimizado",
                "materialization_mode": "prod",
                "materialize after dump": True,
            },
        ),
    ],
    filters=[filters.is_weekday],
    adjustments=[adjustments.next_weekday],
)

schedule_equipes = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=1),
            start_date=datetime(2022, 1, 1, 6, 0),
            labels=[
                constants.BASEDOSDADOS_DEV_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_bd_indicadores",
                "table_id": "equipes",
                "sheet_id": "1gLJyoxiFeIRn7FKiP3Fpbr04bScVuhmF",
                "sheet_name": "equipes",
                "materialization_mode": "dev",
                "materialize after dump": True,
            },
        ),
    ],
    filters=[filters.is_weekday],
    adjustments=[adjustments.next_weekday],
)

schedule_pessoas = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=1),
            start_date=datetime(2022, 1, 1, 6, 0),
            labels=[
                constants.BASEDOSDADOS_DEV_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_bd_indicadores",
                "table_id": "pessoas",
                "sheet_id": "1cQj9ItJoO_AQElRT2ngpHZXhFCSpQCrV",
                "sheet_name": "pessoas",
                "materialization_mode": "dev",
                "materialize after dump": True,
            },
        ),
    ],
    filters=[filters.is_weekday],
    adjustments=[adjustments.next_weekday],
)
