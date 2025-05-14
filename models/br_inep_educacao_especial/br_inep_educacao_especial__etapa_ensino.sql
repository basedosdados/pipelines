{{
    config(
        alias="etapa_ensino",
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
    safe_cast(
        case
            when etapa_ensino = 'Ensino Médio Normal/Magistério'
            then 'Ensino Médio Normal - Magistério'
            else etapa_ensino
        end as string
    ) etapa_ensino,
    safe_cast(quantidade_matricula as numeric) quantidade_matricula
from {{ set_datalake_project("br_inep_educacao_especial_staging.etapa_ensino") }} as t
