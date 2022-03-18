"""
Schedules for br_ibge_inpc
"""

from datetime import timedelta, datetime
from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock
from pipelines.constants import constants

br_ibge_inpc_mes_categoria_brasil_every_month = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=30),
            start_date=datetime(2021, 1, 1),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "indice": "inpc",
                "folder": "br/",
                "dataset_id": "br_ibge_inpc",
                "table_id": "mes_categoria_brasil",
            },
        )
    ]
)

br_ibge_inpc_mes_categoria_rm_every_month = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=30),
            start_date=datetime(2021, 1, 1),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "indice": "inpc",
                "folder": "rm/",
                "dataset_id": "br_ibge_inpc",
                "table_id": "mes_categoria_rm",
            },
        )
    ]
)

br_ibge_inpc_mes_categoria_municipio_every_month = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=30),
            start_date=datetime(2021, 1, 1),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "indice": "inpc",
                "folder": "mun/",
                "dataset_id": "br_ibge_inpc",
                "table_id": "mes_categoria_municipio",
            },
        )
    ]
)

br_ibge_inpc_mes_brasil_every_month = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=30),
            start_date=datetime(2021, 1, 1),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "indice": "inpc",
                "folder": "mes/",
                "dataset_id": "br_ibge_inpc",
                "table_id": "mes_brasil",
            },
        )
    ]
)
