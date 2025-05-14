{{
    config(
        alias="bioma",
        schema="br_geobr_mapas",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2004, "end": 2019, "interval": 15},
        },
    )
}}
select
    safe_cast(year as int64) ano,
    safe_cast(id_bioma as string) id_bioma,
    safe_cast(nome_bioma as string) nome_bioma,
    safe.st_geogfromtext(geometria) geometria
from {{ set_datalake_project("br_geobr_mapas_staging.bioma") }} as t
