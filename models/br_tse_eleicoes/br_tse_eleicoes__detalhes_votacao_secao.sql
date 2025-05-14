{{
    config(
        schema="br_tse_eleicoes",
        alias="detalhes_votacao_secao",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 1994, "end": 2024, "interval": 2},
        },
        cluster_by=["sigla_uf"],
    )
}}

select
    safe_cast(ano as int64) ano,
    safe_cast(turno as int64) turno,
    safe_cast(id_eleicao as string) id_eleicao,
    safe_cast(tipo_eleicao as string) tipo_eleicao,
    safe_cast(data_eleicao as date) data_eleicao,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(id_municipio_tse as string) id_municipio_tse,
    safe_cast(zona as string) zona,
    safe_cast(secao as string) secao,
    safe_cast(cargo as string) cargo,
    safe_cast(aptos as int64) aptos,
    safe_cast(comparecimento as int64) comparecimento,
    safe_cast(abstencoes as int64) abstencoes,
    safe_cast(votos_nominais as int64) votos_nominais,
    safe_cast(votos_brancos as int64) votos_brancos,
    safe_cast(votos_nulos as int64) votos_nulos,
    safe_cast(votos_legenda as int64) votos_legenda,
    safe_cast(votos_nulos_apu_sep as int64) votos_nulos_apu_sep,
    safe_cast(proporcao_comparecimento as float64) proporcao_comparecimento,
    safe_cast(proporcao_votos_nominais as float64) proporcao_votos_nominais,
    safe_cast(proporcao_votos_legenda as float64) proporcao_votos_legenda,
    safe_cast(proporcao_votos_brancos as float64) proporcao_votos_brancos,
    safe_cast(proporcao_votos_nulos as float64) proporcao_votos_nulos
from {{ set_datalake_project("br_tse_eleicoes_staging.detalhes_votacao_secao") }} as t
