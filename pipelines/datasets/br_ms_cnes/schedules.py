# -*- coding: utf-8 -*-
"""
Schedules for br_ms_cnes
"""

from prefect.schedules import Schedule, filters, adjustments
from prefect.schedules.clocks import CronClock
from datetime import datetime
from pipelines.constants import constants


schedule_br_ms_cnes_estabelecimento = Schedule(
    clocks=[
        CronClock(
            cron="0 0 * * *",  # every day at midnight
            start_date=datetime(2023, 9, 1, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_ms_cnes",
                "table_id": "estabelecimento",
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


schedule_br_ms_cnes_profissional = Schedule(
    clocks=[
        CronClock(
            cron="0 0 * * *",  # every day at midnight
            start_date=datetime(2023, 9, 1, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_ms_cnes",
                "table_id": "profissional",
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


schedule_br_ms_cnes_equipe = Schedule(
    clocks=[
        CronClock(
            cron="0 0 * * *",  # every day at midnight
            start_date=datetime(2023, 9, 1, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_ms_cnes",
                "table_id": "equipe",
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

schedule_br_ms_cnes_leito = Schedule(
    clocks=[
        CronClock(
            cron="0 0 * * *",  # every day at midnight
            start_date=datetime(2023, 9, 1, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_ms_cnes",
                "table_id": "leito",
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

schedule_br_ms_cnes_equipamento = Schedule(
    clocks=[
        CronClock(
            cron="0 0 * * *",  # every day at midnight
            start_date=datetime(2023, 9, 1, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_ms_cnes",
                "table_id": "equipamento",
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

schedule_br_ms_cnes_estabelecimento_ensino = Schedule(
    cclocks=[
        CronClock(
            cron="0 0 * * *",  # every day at midnight
            start_date=datetime(2023, 9, 1, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_ms_cnes",
                "table_id": "estabelecimento_ensino",
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

schedule_br_ms_cnes_dados_complementares = Schedule(
    clocks=[
        CronClock(
            cron="0 0 * * *",  # every day at midnight
            start_date=datetime(2023, 9, 1, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_ms_cnes",
                "table_id": "dados_complementares",
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

schedule_br_ms_cnes_estabelecimento_filantropico = Schedule(
    clocks=[
        CronClock(
            cron="0 0 * * *",  # every day at midnight
            start_date=datetime(2023, 9, 1, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_ms_cnes",
                "table_id": "estabelecimento_filantropico",
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
schedule_br_ms_cnes_gestao_metas = Schedule(
    clocks=[
        CronClock(
            cron="0 0 * * *",  # every day at midnight
            start_date=datetime(2023, 9, 1, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_ms_cnes",
                "table_id": "gestao_metas",
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


schedule_br_ms_cnes_habilitacao = Schedule(
    clocks=[
        CronClock(
            cron="0 0 * * *",  # every day at midnight
            start_date=datetime(2023, 9, 1, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_ms_cnes",
                "table_id": "habilitacao",
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

schedule_br_ms_cnes_incentivos = Schedule(
    clocks=[
        CronClock(
            cron="0 0 * * *",  # every day at midnight
            start_date=datetime(2023, 9, 1, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_ms_cnes",
                "table_id": "incentivos",
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

schedule_br_ms_cnes_regra_contratual = Schedule(
    clocks=[
        CronClock(
            cron="0 0 * * *",  # every day at midnight
            start_date=datetime(2023, 9, 1, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_ms_cnes",
                "table_id": "regra_contratual",
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


schedule_br_ms_cnes_servico_especializado = Schedule(
    clocks=[
        CronClock(
            cron="0 0 * * *",  # every day at midnight
            start_date=datetime(2023, 9, 1, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_ms_cnes",
                "table_id": "servico_especializado",
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
