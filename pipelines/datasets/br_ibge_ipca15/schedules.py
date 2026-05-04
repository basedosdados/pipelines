# Referência dos dias de publicação: https://www.ibge.gov.br/calendario-indicadores-novoportal.html
from datetime import datetime

from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock

from pipelines.constants import constants

schedule_br_ibge_ipca15_mes_categoria_brasil = Schedule(
    clocks=[
        CronClock(
            cron="30 13 23,24,25,26,27 * *",  # “At 13:30 on day-of-month 8, 9, 10, 11, 12, and 13.”
            start_date=datetime(2023, 10, 6, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_ibge_ipca15",
                "table_id": "mes_categoria_brasil",
                "target": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "update_metadata": True,
            },
        )
    ],
)

schedule_br_ibge_ipca15_mes_categoria_rm = Schedule(
    clocks=[
        CronClock(
            cron="20 13 23,24,25,26,27 * *",  # “At 13:20 on day-of-month 8, 9, 10, 11, 12, and 13.”
            start_date=datetime(2023, 10, 6, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_ibge_ipca15",
                "table_id": "mes_categoria_rm",
                "target": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "update_metadata": True,
            },
        )
    ],
)


schedule_br_ibge_ipca15_mes_categoria_municipio = Schedule(
    clocks=[
        CronClock(
            cron="10 13 23,24,25,26,27 * *",  # “At 13:10 on day-of-month 8, 9, 10, 11, 12, and 13.”
            start_date=datetime(2023, 10, 6, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_ibge_ipca15",
                "table_id": "mes_categoria_municipio",
                "target": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "update_metadata": True,
            },
        )
    ],
)


schedule_br_ibge_ipca15_mes_brasil = Schedule(
    clocks=[
        CronClock(
            cron="15 13 23,24,25,26,27 * *",  # “At 13:15 on day-of-month 8, 9, 10, 11, 12, and 13.”
            start_date=datetime(2023, 10, 6, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_ibge_ipca15",
                "table_id": "mes_brasil",
                "target": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "update_metadata": True,
            },
        )
    ],
)
