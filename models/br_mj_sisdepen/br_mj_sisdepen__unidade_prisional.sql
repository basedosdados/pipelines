{{
    config(
        schema="br_mj_sisdepen",
        alias="unidade_prisional",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2016, "end": 2030, "interval": 1},
        },
    )
}}


select
    safe_cast(ano as int64) ano,
    safe_cast(semestre as int64) semestre,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(id_unidade as string) id_unidade,
    safe_cast(ciclo as string) ciclo,
    safe_cast(geracao_esquema as string) geracao_esquema,
    safe_cast(nome_unidade as string) nome_unidade,
    safe_cast(nome_unidade_original as string) nome_unidade_original,
    safe_cast(ambito as string) ambito,
    safe_cast(tipo_recolhimento as string) tipo_recolhimento,
    safe_cast(sexo_destinacao_original as string) sexo_destinacao_original,
    safe_cast(tipo_estabelecimento_original as string) tipo_estabelecimento_original,
    safe_cast(gestao as string) gestao,
    safe_cast(data_inauguracao as date) data_inauguracao,
    safe_cast(tipo_regime as string) tipo_regime,
    safe_cast(capacidade_masculina as int64) capacidade_masculina,
    safe_cast(capacidade_feminina as int64) capacidade_feminina,
    safe_cast(capacidade_total as int64) capacidade_total,
    safe_cast(descricao_outro_regime as string) descricao_outro_regime
from {{ set_datalake_project("br_mj_sisdepen_staging.unidade_prisional") }} as t
