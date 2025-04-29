{{
    config(
        alias="microdados_domicilio_1980",
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
    safe_cast(id_distrito as string) id_distrito,
    safe_cast(v201 as string) v201,
    safe_cast(v202 as string) v202,
    safe_cast(v203 as string) v203,
    safe_cast(v204 as string) v204,
    safe_cast(v205 as string) v205,
    safe_cast(v206 as string) v206,
    safe_cast(v207 as string) v207,
    safe_cast(v208 as string) v208,
    safe_cast(v209 as string) v209,
    safe_cast(v602 as int64) v602,
    safe_cast(v212 as int64) v212,
    safe_cast(v213 as int64) v213,
    safe_cast(v214 as string) v214,
    safe_cast(v215 as string) v215,
    safe_cast(v216 as string) v216,
    safe_cast(v217 as string) v217,
    safe_cast(v218 as string) v218,
    safe_cast(v219 as string) v219,
    safe_cast(v220 as string) v220,
    safe_cast(v221 as string) v221,
    safe_cast(v198 as int64) v198,
    safe_cast(v603 as int64) v603,
    safe_cast(v598 as string) v598
from
    {{
        set_datalake_project(
            "br_ibge_censo_demografico_staging.microdados_domicilio_1980"
        )
    }} t
