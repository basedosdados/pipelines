{{
    config(
        schema="br_mapbiomas_estatisticas",
        alias="cobertura_municipio_classe",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 1985, "end": 2030, "interval": 1},
        },
    )
}}


select
    safe_cast(ano as int64) ano,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(id_classe as string) id_classe,
    safe_cast(bioma as string) bioma,
    safe_cast(nivel_1 as string) nivel_1,
    safe_cast(nivel_2 as string) nivel_2,
    safe_cast(nivel_3 as string) nivel_3,
    safe_cast(nivel_4 as string) nivel_4,
    safe_cast(area as float64) area
from
    {{
        set_datalake_project(
            "br_mapbiomas_estatisticas_staging.cobertura_municipio_classe"
        )
    }} as t
