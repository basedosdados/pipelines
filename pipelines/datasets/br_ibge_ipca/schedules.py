# Referência dos dias de publicação: https://www.ibge.gov.br/calendario-indicadores-novoportal.html


from datetime import datetime

from prefect.schedules import Schedule, adjustments, filters
from prefect.schedules.clocks import CronClock

from pipelines.constants import constants

schedule_br_ibge_ipca_mes_categoria_brasil = Schedule(
    clocks=[
        CronClock(
            cron="30 14 8,9,10,11,12,13 * *",  # “At 14:30 on day-of-month 8, 9, 10, 11, 12, and 13.”
            start_date=datetime(2023, 10, 6, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_ibge_ipca",
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

schedule_br_ibge_ipca_mes_categoria_rm = Schedule(
    clocks=[
        CronClock(
            cron="20 14 8,9,10,11,12,13 * *",  # “At 14:20 on day-of-month 8, 9, 10, 11, 12, and 13.”
            start_date=datetime(2023, 10, 6, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_ibge_ipca",
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


schedule_br_ibge_ipca_mes_categoria_municipio = Schedule(
    clocks=[
        CronClock(
            cron="50 14 8,9,10,11,12,13 * *",  # “At 14:50 on day-of-month 8, 9, 10, 11, 12, and 13.”
            start_date=datetime(2023, 10, 6, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_ibge_ipca",
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


schedule_br_ibge_ipca_mes_brasil = Schedule(
    clocks=[
        CronClock(
            cron="40 14 8,9,10,11,12,13 * *",  # “At 14:40 on day-of-month 8, 9, 10, 11, 12, and 13.”
            start_date=datetime(2023, 10, 6, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_ibge_ipca",
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
