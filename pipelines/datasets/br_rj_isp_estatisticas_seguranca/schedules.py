# -*- coding: utf-8 -*-
"""
Schedules for br_rj_isp_estatisticas_seguranca
"""

from datetime import timedelta, datetime
from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock
from pipelines.constants import constants

# ! Schedules tabela evolucao_mensal_cisp
every_month_evolucao_mensal_cisp = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=15),
            start_date=datetime(2023, 4, 25, 10, 0, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_rj_isp_estatisticas_seguranca",  # ! dataset_id do dataset que será executado
                "table_id": "evolucao_mensal_cisp",  # ! table_id do dataset que será executado
                "materialization_mode": "dev",  # ! Aonde o dataset será materializado (dev, prod ou prod-staging)
                "materialize_after_dump": True,  # ! Se o dataset será materializado após o dump
                "dbt_alias": False,
            },
        ),
    ]
)

# ! Schedules para tabela taxa_evolucao_mensal_uf
every_month_taxa_evolucao_mensal_uf = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=15),
            start_date=datetime(2023, 4, 25, 10, 5, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_rj_isp_estatisticas_seguranca",  # ! dataset_id do dataset que será executado
                "table_id": "taxa_evolucao_mensal_uf",  # ! table_id do dataset que será executado
                "materialization_mode": "dev",  # ! Aonde o dataset será materializado (dev, prod ou prod-staging)
                "materialize_after_dump": True,  # ! Se o dataset será materializado após o dump
                "dbt_alias": False,
            },
        ),
    ]
)

# ! Schedules para tabela taxa_evolucao_mensal_municipio
every_month_taxa_evolucao_mensal_municipio = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=15),
            start_date=datetime(2023, 4, 25, 10, 10, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_rj_isp_estatisticas_seguranca",  # ! dataset_id do dataset que será executado
                "table_id": "taxa_evolucao_mensal_municipio",  # ! table_id do dataset que será executado
                "materialization_mode": "dev",  # ! Aonde o dataset será materializado (dev, prod ou prod-staging)
                "materialize_after_dump": True,  # ! Se o dataset será materializado após o dump
                "dbt_alias": False,
            },
        ),
    ]
)

# ! Schedules para tabela feminicidio_mensal_cisp
every_month_feminicidio_mensal_cisp = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=15),
            start_date=datetime(2023, 4, 25, 10, 15, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_rj_isp_estatisticas_seguranca",  # ! dataset_id do dataset que será executado
                "table_id": "feminicidio_mensal_cisp",  # ! table_id do dataset que será executado
                "materialization_mode": "dev",  # ! Aonde o dataset será materializado (dev, prod ou prod-staging)
                "materialize_after_dump": True,  # ! Se o dataset será materializado após o dump
                "dbt_alias": False,
            },
        ),
    ]
)

# ! Schedules para tabela evolucao_policial_morto_servico_mensal
every_month_evolucao_policial_morto_servico_mensal = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=15),
            start_date=datetime(2023, 4, 25, 10, 20, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_rj_isp_estatisticas_seguranca",  # ! dataset_id do dataset que será executado
                "table_id": "evolucao_policial_morto_servico_mensal",  # ! table_id do dataset que será executado
                "materialization_mode": "dev",  # ! Aonde o dataset será materializado (dev, prod ou prod-staging)
                "materialize_after_dump": True,  # ! Se o dataset será materializado após o dump
                "dbt_alias": False,
            },
        ),
    ]
)


# ! Schedules para tabela armas_apreendidas_mensal
every_month_armas_apreendidas_mensal = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=15),
            start_date=datetime(2023, 4, 25, 10, 25, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_rj_isp_estatisticas_seguranca",  # ! dataset_id do dataset que será executado
                "table_id": "armas_apreendidas_mensal",  # ! table_id do dataset que será executado
                "materialization_mode": "dev",  # ! Aonde o dataset será materializado (dev, prod ou prod-staging)
                "materialize_after_dump": True,  # ! Se o dataset será materializado após o dump
                "dbt_alias": False,
            },
        ),
    ]
)


# ! Schedules para tabela evolucao_mensal_municipio
every_month_evolucao_mensal_municipio = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=15),
            start_date=datetime(2023, 4, 25, 10, 30, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_rj_isp_estatisticas_seguranca",  # ! dataset_id do dataset que será executado
                "table_id": "evolucao_mensal_municipio",  # ! table_id do dataset que será executado
                "materialization_mode": "dev",  # ! Aonde o dataset será materializado (dev, prod ou prod-staging)
                "materialize_after_dump": True,  # ! Se o dataset será materializado após o dump
                "dbt_alias": False,
            },
        ),
    ]
)

# ! Schedules para tabela evolucao_mensal_uf
every_month_evolucao_mensal_uf = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=15),
            start_date=datetime(2023, 4, 25, 10, 30, 0),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_rj_isp_estatisticas_seguranca",  # ! dataset_id do dataset que será executado
                "table_id": "evolucao_mensal_uf",  # ! table_id do dataset que será executado
                "materialization_mode": "dev",  # ! Aonde o dataset será materializado (dev, prod ou prod-staging)
                "materialize_after_dump": True,  # ! Se o dataset será materializado após o dump
                "dbt_alias": False,
            },
        ),
    ]
)