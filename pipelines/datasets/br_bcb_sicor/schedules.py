"""
Schedules for br_bcb_sicor
"""

from datetime import datetime

from prefect.schedules import Schedule, adjustments, filters
from prefect.schedules.clocks import CronClock

from pipelines.constants import constants

every_day_microdados_operacao = Schedule(
    clocks=[
        CronClock(
            cron="5 4 * * 1-5",  # “At 04:05 on every day-of-week from Monday through Friday.”
            start_date=datetime(2026, 3, 3, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_bcb_sicor",
                "table_id": "microdados_operacao",
                "target": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "update_metadata": True,
                "append_overwrite": "append_overwrite",
            },
        )
    ],
    filters=[filters.is_weekday],
    adjustments=[adjustments.next_weekday],
)

every_day_microdados_saldo = Schedule(
    clocks=[
        CronClock(
            cron="15 4 * * 1-5",  # “At 04:15 on every day-of-week from Monday through Friday.”
            start_date=datetime(2026, 3, 3, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_bcb_sicor",
                "table_id": "microdados_saldo",
                "target": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "update_metadata": True,
                "append_overwrite": "append_overwrite",
            },
        )
    ],
    filters=[filters.is_weekday],
    adjustments=[adjustments.next_weekday],
)

every_day_microdados_liberacao = Schedule(
    clocks=[
        CronClock(
            cron="25 4 * * 1-5",  # “At 04:25 on every day-of-week from Monday through Friday.”
            start_date=datetime(2026, 3, 3, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_bcb_sicor",
                "table_id": "microdados_liberacao",
                "target": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "update_metadata": True,
                "append_overwrite": "overwrite",
            },
        )
    ],
    filters=[filters.is_weekday],
    adjustments=[adjustments.next_weekday],
)

every_day_microdados_recurso_publico_complemento_operacao = Schedule(
    clocks=[
        CronClock(
            cron="35 4 * * 1-5",  # “At 04:35 on every day-of-week from Monday through Friday.”
            start_date=datetime(2026, 3, 3, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_bcb_sicor",
                "table_id": "microdados_recurso_publico_complemento_operacao",
                "target": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "update_metadata": True,
                "append_overwrite": "overwrite",
            },
        )
    ],
    filters=[filters.is_weekday],
    adjustments=[adjustments.next_weekday],
)

every_day_microdados_recurso_publico_cooperado = Schedule(
    clocks=[
        CronClock(
            cron="45 4 * * 1-5",  # “At 04:45 on every day-of-week from Monday through Friday.”
            start_date=datetime(2026, 3, 3, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_bcb_sicor",
                "table_id": "microdados_recurso_publico_cooperado",
                "target": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "update_metadata": True,
                "append_overwrite": "overwrite",
            },
        )
    ],
    filters=[filters.is_weekday],
    adjustments=[adjustments.next_weekday],
)

every_day_microdados_recurso_publico_gleba = Schedule(
    clocks=[
        CronClock(
            cron="55 4 * * 1-5",  # “At 04:55 on every day-of-week from Monday through Friday.”
            start_date=datetime(2026, 3, 3, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_bcb_sicor",
                "table_id": "microdados_recurso_publico_gleba",
                "target": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "update_metadata": True,
                "append_overwrite": "overwrite",
            },
        )
    ],
    filters=[filters.is_weekday],
    adjustments=[adjustments.next_weekday],
)

every_day_microdados_recurso_publico_mutuario = Schedule(
    clocks=[
        CronClock(
            cron="5 5 * * 1-5",  # “At 05:05 on every day-of-week from Monday through Friday.”
            start_date=datetime(2026, 3, 3, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_bcb_sicor",
                "table_id": "microdados_recurso_publico_mutuario",
                "target": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "update_metadata": True,
                "append_overwrite": "overwrite",
            },
        )
    ],
    filters=[filters.is_weekday],
    adjustments=[adjustments.next_weekday],
)

every_day_microdados_recurso_publico_propriedade = Schedule(
    clocks=[
        CronClock(
            cron="15 5 * * 1-5",  # “At 05:15 on every day-of-week from Monday through Friday.”
            start_date=datetime(2026, 3, 3, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_bcb_sicor",
                "table_id": "microdados_recurso_publico_propriedade",
                "target": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "update_metadata": True,
                "append_overwrite": "overwrite",
            },
        )
    ],
    filters=[filters.is_weekday],
    adjustments=[adjustments.next_weekday],
)

every_day_operacoes_desclassificadas = Schedule(
    clocks=[
        CronClock(
            cron="25 5 * * 1-5",  # “At 05:25 on every day-of-week from Monday through Friday.”
            start_date=datetime(2026, 3, 3, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_bcb_sicor",
                "table_id": "operacoes_desclassificadas",
                "target": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "update_metadata": True,
                "append_overwrite": "overwrite",
            },
        )
    ],
    filters=[filters.is_weekday],
    adjustments=[adjustments.next_weekday],
)

every_day_empreendimento = Schedule(
    clocks=[
        CronClock(
            cron="35 5 * * 1-5",  # “At 05:35 on every day-of-week from Monday through Friday.”
            start_date=datetime(2026, 3, 3, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_bcb_sicor",
                "table_id": "empreendimento",
                "target": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "update_metadata": True,
                "append_overwrite": "overwrite",
            },
        )
    ],
    filters=[filters.is_weekday],
    adjustments=[adjustments.next_weekday],
)
