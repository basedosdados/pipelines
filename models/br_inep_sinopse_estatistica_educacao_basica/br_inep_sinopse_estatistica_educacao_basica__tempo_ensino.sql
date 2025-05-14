{{
    config(
        alias="tempo_ensino",
        schema="br_inep_sinopse_estatistica_educacao_basica",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2007, "end": 2024, "interval": 1},
        },
        cluster_by="sigla_uf",
    )
}}

select
    safe_cast(ano as int64) ano,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(rede as string) rede,
    safe_cast(tempo_ensino as string) tempo_ensino,
    safe_cast(etapa_ensino as string) etapa_ensino,
    safe_cast(quantidade_matricula as int64) quantidade_matricula,
from
    `basedosdados-staging.br_inep_sinopse_estatistica_educacao_basica_staging.tempo_ensino`
    as t
