{{
    config(
        alias="pais",
        schema="br_geobr_mapas",
        materialized="table",
    )
}}
select safe.st_geogfromtext(geometria) geometria,
from {{ set_datalake_project("br_geobr_mapas_staging.pais") }} as t
