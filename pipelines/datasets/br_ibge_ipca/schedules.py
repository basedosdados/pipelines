"""
Schedules for br_ibge_ipca
"""

from datetime import timedelta, datetime
from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock
from pipelines.constants import constants

br_ibge_ipca_mes_categoria_brasil_every_month = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=30),
            start_date=datetime(2021, 1, 1),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "indice": "ipca",
                "folder": "br/",
                "dataset_id": "br_ibge_ipca",
                "table_id": "mes_categoria_brasil",
            },
        )
    ]
)

br_ibge_ipca_mes_categoria_rm_every_month = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=30),
            start_date=datetime(2021, 1, 1),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "indice": "ipca",
                "folder": "rm/",
                "dataset_id": "br_ibge_ipca",
                "table_id": "mes_categoria_rm",
            },
        )
    ]
)

br_ibge_ipca_mes_categoria_municipio_every_month = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=30),
            start_date=datetime(2021, 1, 1),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "indice": "ipca",
                "folder": "mun/",
                "dataset_id": "br_ibge_ipca",
                "table_id": "mes_categoria_municipio",
            },
        )
    ]
)

br_ibge_ipca_mes_brasil_every_month = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=30),
            start_date=datetime(2021, 1, 1),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "indice": "ipca",
                "folder": "mes/",
                "dataset_id": "br_ibge_ipca",
                "table_id": "mes_brasil",
            },
        )
    ]
)
