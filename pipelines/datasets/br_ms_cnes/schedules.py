# -*- coding: utf-8 -*-
"""
Schedules for br_ms_cnes
"""

from datetime import datetime

from prefect.schedules import Schedule, adjustments, filters
from prefect.schedules.clocks import CronClock

from pipelines.constants import constants

schedule_br_ms_cnes_estabelecimento = Schedule(
    clocks=[
        CronClock(
            cron="0 9 * * *",  # every day at 9:00
            start_date=datetime(2024, 1, 1, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_ms_cnes",
                "table_id": "estabelecimento",
                "target": "prod",
                "materialize_after_dump": True,
                "update_metadata": True,
                "dbt_alias": True,
            },
        )
    ],
    filters=[filters.is_weekday],
    adjustments=[adjustments.next_weekday],
)

# todo selecionar outro horário
schedule_br_ms_cnes_profissional = Schedule(
    clocks=[
        CronClock(
            cron="30 6 * * *",  # every day at 18:30
            start_date=datetime(2024, 1, 1, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_ms_cnes",
                "table_id": "profissional",
                "target": "prod",
                "materialize_after_dump": True,
                "update_metadata": True,
                "dbt_alias": True,
            },
        )
    ],
    filters=[filters.is_weekday],
    adjustments=[adjustments.next_weekday],
)


schedule_br_ms_cnes_equipe = Schedule(
    clocks=[
        CronClock(
            cron="30 9 * * *",  # every day 9:30
            start_date=datetime(2024, 1, 1, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_ms_cnes",
                "table_id": "equipe",
                "target": "prod",
                "materialize_after_dump": True,
                "update_metadata": True,
                "dbt_alias": True,
            },
        )
    ],
    filters=[filters.is_weekday],
    adjustments=[adjustments.next_weekday],
)

schedule_br_ms_cnes_leito = Schedule(
    clocks=[
        CronClock(
            cron="0 10 * * *",  # every day at 10:00
            start_date=datetime(2024, 1, 1, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_ms_cnes",
                "table_id": "leito",
                "target": "prod",
                "materialize_after_dump": True,
                "update_metadata": True,
                "dbt_alias": True,
            },
        )
    ],
    filters=[filters.is_weekday],
    adjustments=[adjustments.next_weekday],
)

schedule_br_ms_cnes_equipamento = Schedule(
    clocks=[
        CronClock(
            cron="30 10 * * *",  # every day at 10:30
            start_date=datetime(2024, 1, 1, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_ms_cnes",
                "table_id": "equipamento",
                "target": "prod",
                "materialize_after_dump": True,
                "update_metadata": True,
                "dbt_alias": True,
            },
        )
    ],
    filters=[filters.is_weekday],
    adjustments=[adjustments.next_weekday],
)

schedule_br_ms_cnes_estabelecimento_ensino = Schedule(
    clocks=[
        CronClock(
            cron="45 10 * * *",  # every day at 10:45
            start_date=datetime(2024, 1, 1, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_ms_cnes",
                "table_id": "estabelecimento_ensino",
                "target": "prod",
                "materialize_after_dump": True,
                "update_metadata": True,
                "dbt_alias": True,
            },
        )
    ],
    filters=[filters.is_weekday],
    adjustments=[adjustments.next_weekday],
)

schedule_br_ms_cnes_dados_complementares = Schedule(
    clocks=[
        CronClock(
            cron="0 11 * * *",  # every day 11:00
            start_date=datetime(2024, 1, 1, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_ms_cnes",
                "table_id": "dados_complementares",
                "target": "prod",
                "materialize_after_dump": True,
                "update_metadata": True,
                "dbt_alias": True,
            },
        )
    ],
    filters=[filters.is_weekday],
    adjustments=[adjustments.next_weekday],
)

schedule_br_ms_cnes_estabelecimento_filantropico = Schedule(
    clocks=[
        CronClock(
            cron="15 11 * * *",  # every day at 11:15
            start_date=datetime(2024, 1, 1, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_ms_cnes",
                "table_id": "estabelecimento_filantropico",
                "target": "prod",
                "materialize_after_dump": True,
                "update_metadata": True,
                "dbt_alias": True,
            },
        )
    ],
    filters=[filters.is_weekday],
    adjustments=[adjustments.next_weekday],
)
schedule_br_ms_cnes_gestao_metas = Schedule(
    clocks=[
        CronClock(
            cron="30 11 * * *",  # every day at 11:30
            start_date=datetime(2024, 1, 1, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_ms_cnes",
                "table_id": "gestao_metas",
                "target": "prod",
                "materialize_after_dump": True,
                "update_metadata": True,
                "dbt_alias": True,
            },
        )
    ],
    filters=[filters.is_weekday],
    adjustments=[adjustments.next_weekday],
)


schedule_br_ms_cnes_habilitacao = Schedule(
    clocks=[
        CronClock(
            cron="45 11 * * *",  # every day at 11:45
            start_date=datetime(2024, 1, 1, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_ms_cnes",
                "table_id": "habilitacao",
                "target": "prod",
                "materialize_after_dump": True,
                "update_metadata": True,
                "dbt_alias": True,
            },
        )
    ],
    filters=[filters.is_weekday],
    adjustments=[adjustments.next_weekday],
)

schedule_br_ms_cnes_incentivos = Schedule(
    clocks=[
        CronClock(
            cron="45 11 * * *",  # every day at 11:45
            start_date=datetime(2024, 1, 1, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_ms_cnes",
                "table_id": "incentivos",
                "target": "prod",
                "materialize_after_dump": True,
                "update_metadata": True,
                "dbt_alias": True,
            },
        )
    ],
    filters=[filters.is_weekday],
    adjustments=[adjustments.next_weekday],
)

schedule_br_ms_cnes_regra_contratual = Schedule(
    clocks=[
        CronClock(
            cron="0 12 * * *",  # every day at 12:00
            start_date=datetime(2024, 1, 1, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_ms_cnes",
                "table_id": "regra_contratual",
                "target": "prod",
                "materialize_after_dump": True,
                "update_metadata": True,
                "dbt_alias": True,
            },
        )
    ],
    filters=[filters.is_weekday],
    adjustments=[adjustments.next_weekday],
)


schedule_br_ms_cnes_servico_especializado = Schedule(
    clocks=[
        CronClock(
            cron="30 12 * * *",  # every day at 12:30
            start_date=datetime(2024, 1, 1, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_ms_cnes",
                "table_id": "servico_especializado",
                "target": "prod",
                "materialize_after_dump": True,
                "update_metadata": True,
                "dbt_alias": True,
            },
        )
    ],
    filters=[filters.is_weekday],
    adjustments=[adjustments.next_weekday],
)
