{{
    config(
        alias="microdados_pessoa_1970",
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
    safe_cast(ordem as string) ordem,
    safe_cast(v001 as string) v001,
    safe_cast(v002 as string) v002,
    safe_cast(v003 as string) v003,
    safe_cast(v022 as string) v022,
    safe_cast(v023 as string) v023,
    safe_cast(v024 as string) v024,
    safe_cast(v025 as string) v025,
    safe_cast(v026 as string) v026,
    safe_cast(v027 as int64) v027,
    safe_cast(v028 as string) v028,
    safe_cast(v029 as string) v029,
    safe_cast(v030 as string) v030,
    safe_cast(v031 as string) v031,
    safe_cast(v032 as string) v032,
    safe_cast(v033 as string) v033,
    safe_cast(v034 as string) v034,
    safe_cast(v035 as string) v035,
    safe_cast(v036 as string) v036,
    safe_cast(v037 as string) v037,
    safe_cast(v038 as string) v038,
    safe_cast(v039 as string) v039,
    safe_cast(v040 as string) v040,
    safe_cast(v041 as int64) v041,
    safe_cast(v042 as string) v042,
    safe_cast(v043 as string) v043,
    safe_cast(v044 as int64) v044,
    safe_cast(v045 as int64) v045,
    safe_cast(v046 as string) v046,
    safe_cast(v047 as string) v047,
    safe_cast(v048 as string) v048,
    safe_cast(v049 as string) v049,
    safe_cast(v050 as int64) v050,
    safe_cast(v051 as string) v051,
    safe_cast(v052 as string) v052,
    safe_cast(v053 as int64) v053,
    safe_cast(v054 as int64) v054
from
    {{
        set_datalake_project(
            "br_ibge_censo_demografico_staging.microdados_pessoa_1970 "
        )
    }} as t
