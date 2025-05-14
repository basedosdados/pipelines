{{
    config(
        alias="docente_aee",
        schema="br_inep_educacao_especial",
        materialized="table",
    )
}}

select
    safe_cast(ano as int64) ano,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(quantidade_docente_regente as numeric) quantidade_docente_regente,
    safe_cast(quantidade_docente_aee as numeric) quantidade_docente_aee,
    safe_cast(
        quantidade_docente_regente_formacao_continuada as int64
    ) quantidade_docente_regente_formacao_continuada,
    safe_cast(
        quantidade_docente_aee_formacao_continuada as int64
    ) quantidade_docente_aee_formacao_continuada,
from {{ set_datalake_project("br_inep_educacao_especial_staging.docente_aee") }} as t
