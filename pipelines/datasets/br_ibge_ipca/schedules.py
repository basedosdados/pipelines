# -*- coding: utf-8 -*-
"""
Schedules for br_ibge_ipca
"""
from pipelines.utils.crawler_ibge_inflacao.utils import generate_inflacao_clocks

br_ibge_ipca_mes_categoria_brasil_every_month = generate_inflacao_clocks(
    parameters={
        "indice": "ipca",
        "folder": "br",
        "dataset_id": "br_ibge_ipca",
        "table_id": "mes_categoria_brasil",
        "materialization_mode": "prod",
        "materialize after dump": True,
        "dbt_alias": True,
        "update_metadata": True,
    }
)

br_ibge_ipca_mes_categoria_rm_every_month = generate_inflacao_clocks(
    parameters={
        "indice": "ipca",
        "folder": "rm",
        "dataset_id": "br_ibge_ipca",
        "table_id": "mes_categoria_rm",
        "materialization_mode": "prod",
        "materialize after dump": True,
        "dbt_alias": True,
        "update_metadata": True,
    }
)


br_ibge_ipca_mes_categoria_municipio_every_month = generate_inflacao_clocks(
    parameters={
        "indice": "ipca",
        "folder": "mun",
        "dataset_id": "br_ibge_ipca",
        "table_id": "mes_categoria_municipio",
        "materialization_mode": "prod",
        "materialize after dump": True,
        "dbt_alias": True,
        "update_metadata": True,
    },
)

br_ibge_ipca_mes_brasil_every_month = generate_inflacao_clocks(
    parameters={
        "indice": "ipca",
        "folder": "mes",
        "dataset_id": "br_ibge_ipca",
        "table_id": "mes_brasil",
        "materialization_mode": "prod",
        "materialize after dump": True,
        "dbt_alias": True,
        "update_metadata": True,
    },
)
