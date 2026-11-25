{{
    config(
        schema="br_ufmg_censo_demografico_1872",
        alias="municipio",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 1872, "end": 1877, "interval": 1},
        },
    )
}}


select
    safe_cast(ano as int64) ano,
    safe_cast(id_provincia as string) id_provincia,
    safe_cast(id_municipio_1872 as string) id_municipio_1872,
    safe_cast(nome_municipio as string) nome_municipio
from {{ set_datalake_project("br_ufmg_censo_demografico_1872_staging.municipio") }} as t
