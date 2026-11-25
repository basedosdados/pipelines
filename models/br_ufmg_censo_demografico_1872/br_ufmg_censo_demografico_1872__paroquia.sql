{{
    config(
        schema="br_ufmg_censo_demografico_1872",
        alias="paroquia",
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
    safe_cast(id_paroquia as string) id_paroquia,
    safe_cast(nome_paroquia as string) nome_paroquia
from {{ set_datalake_project("br_ufmg_censo_demografico_1872_staging.paroquia") }} as t
