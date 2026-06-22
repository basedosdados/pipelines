{{
    config(
        schema="br_tse_eleicoes",
        alias="resultados_candidato_municipio",
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
-- ref() impõe a ordem zona -> municipio e resolve dev/prod pelo target.
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
    titulo_eleitoral_candidato,
    sequencial_candidato,
    numero_candidato,
    resultado,
    sum(votos) as votos
from {{ ref("br_tse_eleicoes__resultados_candidato_municipio_zona") }}
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15
