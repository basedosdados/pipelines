{{
    config(
        alias="setor_censitario_registro_civil_2010",
        schema="br_ibge_censo_demografico",
        materialized="table",
        partition_by={
            "field": "sigla_uf",
            "data_type": "string",
        },
    )
}}
select
    safe_cast(id_setor_censitario as string) id_setor_censitario,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(v001 as int64) v001,
    safe_cast(v002 as int64) v002,
    safe_cast(v003 as int64) v003
from
    {{
        set_datalake_project(
            "br_ibge_censo_demografico_staging.setor_censitario_registro_civil_2010"
        )
    }} as t
