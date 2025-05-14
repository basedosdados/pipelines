{{
    config(
        alias="microdados_domicilio_1970",
        schema="br_ibge_censo_demografico",
        materialized="table",
        partition_by={
            "field": "sigla_uf",
            "data_type": "string",
        },
    )
}}

select
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(id_domicilio as string) id_domicilio,
    safe_cast(numero_familia as int64) numero_familia,
    safe_cast(v001 as string) v001,
    safe_cast(v002 as string) v002,
    safe_cast(v003 as string) v003,
    safe_cast(v004 as string) v004,
    safe_cast(v005 as int64) v005,
    safe_cast(v006 as string) v006,
    safe_cast(v007 as string) v007,
    safe_cast(v008 as string) v008,
    safe_cast(v009 as string) v009,
    safe_cast(v010 as string) v010,
    safe_cast(v011 as string) v011,
    safe_cast(v012 as string) v012,
    safe_cast(v013 as string) v013,
    safe_cast(v014 as string) v014,
    safe_cast(v015 as string) v015,
    safe_cast(v016 as string) v016,
    safe_cast(v017 as string) v017,
    safe_cast(v018 as string) v018,
    safe_cast(v019 as string) v019,
    safe_cast(v020 as int64) v020,
    safe_cast(v021 as int64) v021,
    safe_cast(v054 as int64) v054
from
    {{
        set_datalake_project(
            "br_ibge_censo_demografico_staging.microdados_domicilio_1970"
        )
    }} t
