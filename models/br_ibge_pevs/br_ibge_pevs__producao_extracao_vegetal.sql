{{
    config(
        alias="producao_extracao_vegetal",
        schema="br_ibge_pevs",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 1986, "end": 2022, "interval": 1},
        },
        cluster_by=["id_municipio"],
    )
}}

select
    safe_cast(ano as int64) ano,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(tipo_produto as string) tipo_produto,
    safe_cast(produto as string) produto,
    safe_cast(unidade as string) unidade,
    safe_cast(quantidade as int64) quantidade,
    round(safe_cast(valor as float64), 4) valor,
from {{ set_datalake_project("br_ibge_pevs_staging.producao_extracao_vegetal") }}
where
    produto is not null  -- isso faz categorias de agregação caírem
    and quantidade is not null  -- isso faz unidade vazia cair
