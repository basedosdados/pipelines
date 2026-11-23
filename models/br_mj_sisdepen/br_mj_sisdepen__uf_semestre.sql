{{
    config(
        schema="br_mj_sisdepen",
        alias="uf_semestre",
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
    safe_cast(ciclo as string) ciclo,
    safe_cast(geracao_esquema as string) geracao_esquema,
    safe_cast(unidades as int64) unidades,
    safe_cast(populacao_total as int64) populacao_total,
    safe_cast(populacao_masculina as int64) populacao_masculina,
    safe_cast(populacao_feminina as int64) populacao_feminina,
    safe_cast(populacao_provisoria as int64) populacao_provisoria,
    safe_cast(capacidade_total as int64) capacidade_total,
    safe_cast(vagas_desativadas as int64) vagas_desativadas,
    safe_cast(taxa_ocupacao as float64) taxa_ocupacao
from {{ set_datalake_project("br_mj_sisdepen_staging.uf_semestre") }} as t
