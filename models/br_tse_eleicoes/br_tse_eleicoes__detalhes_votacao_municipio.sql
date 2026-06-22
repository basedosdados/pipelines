{{
    config(
        schema="br_tse_eleicoes",
        alias="detalhes_votacao_municipio",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 1994, "end": 2024, "interval": 2},
        },
        cluster_by=["sigla_uf"],
    )
}}
-- Rollup puro em dbt: agrega a tabela zona publicada (drop zona, soma as
-- contagens) e RECOMPUTA as 4 proporções sobre os totais municipais.
with
    agg as (
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
            sum(aptos) as aptos,
            sum(secoes) as secoes,
            sum(secoes_agregadas) as secoes_agregadas,
            sum(aptos_totalizadas) as aptos_totalizadas,
            sum(secoes_totalizadas) as secoes_totalizadas,
            sum(comparecimento) as comparecimento,
            sum(abstencoes) as abstencoes,
            sum(votos_validos) as votos_validos,
            sum(votos_brancos) as votos_brancos,
            sum(votos_nulos) as votos_nulos,
            sum(votos_nominais) as votos_nominais,
            sum(votos_legenda) as votos_legenda
        from {{ ref("br_tse_eleicoes__detalhes_votacao_municipio_zona") }}
        group by 1, 2, 3, 4, 5, 6, 7, 8, 9
    )
select
    *,
    100 * safe_divide(comparecimento, aptos) as proporcao_comparecimento,
    100 * safe_divide(votos_validos, comparecimento) as proporcao_votos_validos,
    100 * safe_divide(votos_brancos, comparecimento) as proporcao_votos_brancos,
    100 * safe_divide(votos_nulos, comparecimento) as proporcao_votos_nulos
from agg
