{{
    config(
        alias="localizacao",
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
    safe_cast(
        case
            when id_municipio = "3201705"
            then "3201605"
            when id_municipio = "4201704"
            then "4201604"
            when id_municipio = "4201753"
            then "4201653"
            when id_municipio = "5201703"
            then "5201603"
            else id_municipio
        end as string
    ) id_municipio,
    safe_cast(rede as string) rede,
    safe_cast(etapa_ensino as string) etapa_ensino,
    safe_cast(localizacao as string) localizacao,
    safe_cast(quantidade_matricula as int64) quantidade_matricula,
from
    `basedosdados-staging.br_inep_sinopse_estatistica_educacao_basica_staging.localizacao`
    as t
