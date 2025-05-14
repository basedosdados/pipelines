{{
    config(
        alias="setor_censitario_domicilio_renda_2010",
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
    safe_cast(v003 as int64) v003,
    safe_cast(v004 as int64) v004,
    safe_cast(v005 as int64) v005,
    safe_cast(v006 as int64) v006,
    safe_cast(v007 as int64) v007,
    safe_cast(v008 as int64) v008,
    safe_cast(v009 as int64) v009,
    safe_cast(v010 as int64) v010,
    safe_cast(v011 as int64) v011,
    safe_cast(v012 as int64) v012,
    safe_cast(v013 as int64) v013,
    safe_cast(v014 as int64) v014
from
    {{
        set_datalake_project(
            "br_ibge_censo_demografico_staging.setor_censitario_domicilio_renda_2010"
        )
    }} as t
