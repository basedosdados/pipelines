# -*- coding: utf-8 -*-
"""
Schedules for br_ibge_inpc
"""
from pipelines.utils.crawler_ibge_inflacao.utils import generate_inflacao_clocks

br_ibge_inpc_mes_categoria_brasil_every_month = generate_inflacao_clocks(
    parameters={
        "indice": "inpc",
        "folder": "br",
        "dataset_id": "br_ibge_inpc",
        "table_id": "mes_categoria_brasil",
    }
)

br_ibge_inpc_mes_categoria_rm_every_month = generate_inflacao_clocks(
    parameters={
        "indice": "inpc",
        "folder": "rm",
        "dataset_id": "br_ibge_inpc",
        "table_id": "mes_categoria_rm",
        "materialization_mode": "dev",
        "materialize after dump": True,
    }
)


br_ibge_inpc_mes_categoria_municipio_every_month = generate_inflacao_clocks(
    parameters={
        "indice": "inpc",
        "folder": "mun",
        "dataset_id": "br_ibge_inpc",
        "table_id": "mes_categoria_municipio",
        "materialization_mode": "dev",
        "materialize after dump": True,
    },
)

br_ibge_inpc_mes_brasil_every_month = generate_inflacao_clocks(
    parameters={
        "indice": "inpc",
        "folder": "mes",
        "dataset_id": "br_ibge_inpc",
        "table_id": "mes_brasil",
        "materialization_mode": "dev",
        "materialize after dump": True,
    },
)
