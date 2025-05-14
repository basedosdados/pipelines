{{
    config(
        alias="setor_censitario_basico_2010",
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
    safe_cast(v001 as float64) v001,
    safe_cast(v002 as float64) v002,
    safe_cast(v003 as float64) v003,
    safe_cast(v004 as float64) v004,
    safe_cast(v005 as float64) v005,
    safe_cast(v006 as float64) v006,
    safe_cast(v007 as float64) v007,
    safe_cast(v008 as float64) v008,
    safe_cast(v009 as float64) v009,
    safe_cast(v010 as float64) v010,
    safe_cast(v011 as float64) v011,
    safe_cast(v012 as float64) v012
from
    {{
        set_datalake_project(
            "br_ibge_censo_demografico_staging.setor_censitario_basico_2010"
        )
    }} t
