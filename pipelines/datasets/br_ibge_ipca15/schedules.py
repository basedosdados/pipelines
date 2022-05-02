"""
Schedules for br_ibge_ipca15
"""
from pipelines.utils.crawler_ibge_inflacao.utils import generate_inflacao_clocks

br_ibge_ipca15_mes_categoria_brasil_every_month = generate_inflacao_clocks(
    parameters={
        "indice": "ip15",
        "folder": "br/",
        "dataset_id": "br_ibge_ipca15",
        "table_id": "mes_categoria_brasil",
    }
)
br_ibge_ipca15_mes_categoria_rm_every_month = generate_inflacao_clocks(
    parameters={
        "indice": "ip15",
        "folder": "rm",
        "dataset_id": "br_ibge_ipca15",
        "table_id": "mes_categoria_rm",
    }
)


br_ibge_ipca15_mes_categoria_municipio_every_month = generate_inflacao_clocks(
    parameters={
        "indice": "ip15",
        "folder": "mun",
        "dataset_id": "br_ibge_ipca15",
        "table_id": "mes_categoria_municipio",
    },
)

br_ibge_ipca15_mes_brasil_every_month = generate_inflacao_clocks(
    parameters={
        "indice": "ip15",
        "folder": "mes",
        "dataset_id": "br_ibge_ipca15",
        "table_id": "mes_brasil",
    },
)
