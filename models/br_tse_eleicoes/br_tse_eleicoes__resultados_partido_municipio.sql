{{
    config(
        schema="br_tse_eleicoes",
        alias="resultados_partido_municipio",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 1994, "end": 2024, "interval": 2},
        },
        cluster_by=["sigla_uf"],
    )
}}
-- Rollup puro em dbt: agrega a tabela zona publicada (drop zona, soma votos).
select
    ano,
    turno,
    id_eleicao,
    tipo_eleicao,
    data_eleicao,
    sigla_uf,
    id_municipio,
    id_municipio_tse,
    cargo,
    numero_partido,
    sigla_partido,
    sum(votos_nominais) as votos_nominais,
    sum(votos_legenda) as votos_legenda
from {{ ref("br_tse_eleicoes__resultados_partido_municipio_zona") }}
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
