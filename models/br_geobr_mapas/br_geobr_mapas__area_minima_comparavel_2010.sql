{{
    config(
        alias="area_minima_comparavel_2010",
        schema="br_geobr_mapas",
        materialized="table",
    )
}}
select
    safe_cast(replace(id_amc, ".0", "") as string) id_amc,
    safe_cast(id_municipio as string) id_municipio,
    safe.st_geogfromtext(geometria) geometria,
from
    {{ set_datalake_project("br_geobr_mapas_staging.area_minima_comparavel_2010") }}
    as t
