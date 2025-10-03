# Referência dos dias de publicação: https://www.ibge.gov.br/calendario-indicadores-novoportal.html

from datetime import datetime

from prefect.schedules import Schedule, adjustments, filters
from prefect.schedules.clocks import CronClock

from pipelines.constants import constants

schedule_br_ibge_inpc_mes_categoria_brasil = Schedule(
    clocks=[
        CronClock(
            cron="50 15 8,9,10,11,12,13 * *",  # “At 15:50 on day-of-month 8, 9, 10, 11, 12, and 13.”
            start_date=datetime(2023, 10, 6, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "indice": "inpc",
                "folder": "br",
                "dataset_id": "br_ibge_inpc",
                "table_id": "mes_categoria_brasil",
                "target": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "update_metadata": True,
            },
        )
    ],
    filters=[filters.is_weekday],
    adjustments=[adjustments.next_weekday],
)

schedule_br_ibge_inpc_mes_categoria_rm = Schedule(
    clocks=[
        CronClock(
            cron="40 15 8,9,10,11,12,13 * *",  # “At 15:40 on day-of-month 8, 9, 10, 11, 12, and 13.”
            start_date=datetime(2023, 10, 6, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_ibge_inpc",
                "table_id": "mes_categoria_rm",
                "target": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "update_metadata": True,
            },
        )
    ],
    filters=[filters.is_weekday],
    adjustments=[adjustments.next_weekday],
)


schedule_br_ibge_inpc_mes_categoria_municipio = Schedule(
    clocks=[
        CronClock(
            cron="30 15 8,9,10,11,12,13 * *",  # “At 15:30 on day-of-month 8, 9, 10, 11, 12, and 13.”
            start_date=datetime(2023, 10, 6, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_ibge_inpc",
                "table_id": "mes_categoria_municipio",
                "target": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "update_metadata": True,
            },
        )
    ],
    filters=[filters.is_weekday],
    adjustments=[adjustments.next_weekday],
)


schedule_br_ibge_inpc_mes_brasil = Schedule(
    clocks=[
        CronClock(
            cron="20 15 8,9,10,11,12,13 * *",  # “At 15:20 on day-of-month 8, 9, 10, 11, 12, and 13.”
            start_date=datetime(2023, 10, 6, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_ibge_inpc",
                "table_id": "mes_brasil",
                "target": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "update_metadata": True,
            },
        )
    ],
    filters=[filters.is_weekday],
    adjustments=[adjustments.next_weekday],
)
