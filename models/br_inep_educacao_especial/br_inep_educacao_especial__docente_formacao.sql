{{
    config(
        alias="docente_formacao",
        schema="br_inep_educacao_especial",
        materialized="table",
    )
}}

select
    safe_cast(ano as int64) ano,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(rede as string) rede,
    safe_cast(
        quantidade_docente_formacao_continuada as numeric
    ) quantidade_docente_formacao_continuada,

from
    {{ set_datalake_project("br_inep_educacao_especial_staging.docente_formacao") }}
    as t
