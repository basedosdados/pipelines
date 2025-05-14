{{
    config(
        alias="matricula_aee",
        schema="br_inep_educacao_especial",
        materialized="table",
    )
}}

select
    safe_cast(ano as int64) ano,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(rede as string) rede,
    safe_cast(quantidade_matricula as numeric) quantidade_matricula,
    safe_cast(quantidade_matricula_aee as numeric) quantidade_matricula_aee,

from {{ set_datalake_project("br_inep_educacao_especial_staging.matricula_aee") }} as t
