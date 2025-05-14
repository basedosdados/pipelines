{{
    config(
        alias="microdados_domicilio_1991",
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
    safe_cast(id_questionario as string) id_questionario,
    safe_cast(peso_amostral as float64) peso_amostral,
    safe_cast(v0109 as string) v0109,
    safe_cast(v1061 as string) v1061,
    safe_cast(v7003 as string) v7003,
    safe_cast(v0111 as int64) v0111,
    safe_cast(v0112 as string) v0112,
    safe_cast(v0201 as string) v0201,
    safe_cast(v2012 as int64) v2012,
    safe_cast(v2013 as string) v2013,
    safe_cast(v2014 as string) v2014,
    safe_cast(v0202 as string) v0202,
    safe_cast(v0203 as string) v0203,
    safe_cast(v0204 as string) v0204,
    safe_cast(v0205 as string) v0205,
    safe_cast(v0206 as string) v0206,
    safe_cast(v0207 as string) v0207,
    safe_cast(v0208 as string) v0208,
    safe_cast(v0209 as int64) v0209,
    safe_cast(v2094 as string) v2094,
    safe_cast(v0210 as string) v0210,
    safe_cast(v0211 as int64) v0211,
    safe_cast(v2111 as int64) v2111,
    safe_cast(v2112 as string) v2112,
    safe_cast(v0212 as int64) v0212,
    safe_cast(v2121 as int64) v2121,
    safe_cast(v2122 as string) v2122,
    safe_cast(v0213 as int64) v0213,
    safe_cast(v0214 as string) v0214,
    safe_cast(v0216 as string) v0216,
    safe_cast(v0217 as string) v0217,
    safe_cast(v0218 as string) v0218,
    safe_cast(v0219 as string) v0219,
    safe_cast(v0220 as string) v0220,
    safe_cast(v0221 as string) v0221,
    safe_cast(v0222 as string) v0222,
    safe_cast(v0223 as string) v0223,
    safe_cast(v0224 as string) v0224,
    safe_cast(v0225 as string) v0225,
    safe_cast(v0226 as string) v0226,
    safe_cast(v0227 as string) v0227
from
    {{
        set_datalake_project(
            "br_ibge_censo_demografico_staging.microdados_domicilio_1991"
        )
    }} t
