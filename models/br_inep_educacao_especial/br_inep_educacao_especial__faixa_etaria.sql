{{
    config(
        alias="faixa_etaria",
        schema="br_inep_educacao_especial",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2007, "end": 2023, "interval": 1},
        },
        cluster_by="sigla_uf",
    )
}}
select
    safe_cast(ano as int64) ano,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(tipo_classe as string) tipo_classe,
    safe_cast(faixa_etaria as string) faixa_etaria,
    safe_cast(quantidade_matricula as numeric) quantidade_matricula,
from {{ project_path("br_inep_educacao_especial_staging.faixa_etaria") }} as t
