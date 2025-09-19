# -*- coding: utf-8 -*-
"""
Schedules for br_rj_isp_estatisticas_seguranca
"""

from datetime import datetime

from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock

from pipelines.constants import constants

every_month_evolucao_mensal_cisp = Schedule(
    clocks=[
        CronClock(
            cron="5 10 * * *",
            start_date=datetime(2023, 4, 25, 10, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_rj_isp_estatisticas_seguranca",
                "table_id": "evolucao_mensal_cisp",
                "target": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
            },
        ),
    ]
)


every_month_feminicidio_mensal_cisp = Schedule(
    clocks=[
        CronClock(
            cron="5 10 * * *",
            start_date=datetime(2023, 4, 25, 10, 15, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_rj_isp_estatisticas_seguranca",
                "table_id": "feminicidio_mensal_cisp",
                "target": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
            },
        ),
    ]
)


every_month_evolucao_policial_morto_servico_mensal = Schedule(
    clocks=[
        CronClock(
            cron="10 10 * * *",
            start_date=datetime(2023, 4, 25, 10, 20, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_rj_isp_estatisticas_seguranca",
                "table_id": "evolucao_policial_morto_servico_mensal",
                "target": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
            },
        ),
    ]
)


every_month_armas_apreendidas_mensal = Schedule(
    clocks=[
        CronClock(
            cron="15 10 * * *",
            start_date=datetime(2023, 4, 25, 10, 25, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_rj_isp_estatisticas_seguranca",
                "table_id": "armas_apreendidas_mensal",
                "target": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
            },
        ),
    ]
)


every_month_evolucao_mensal_municipio = Schedule(
    clocks=[
        CronClock(
            cron="20 10 * * 5",
            start_date=datetime(2023, 4, 25, 10, 30, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_rj_isp_estatisticas_seguranca",
                "table_id": "evolucao_mensal_municipio",
                "target": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
            },
        ),
    ]
)


every_month_evolucao_mensal_uf = Schedule(
    clocks=[
        CronClock(
            cron="25 10 * * *",
            start_date=datetime(2023, 4, 25, 10, 30, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_rj_isp_estatisticas_seguranca",
                "table_id": "evolucao_mensal_uf",
                "target": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
            },
        ),
    ]
)
