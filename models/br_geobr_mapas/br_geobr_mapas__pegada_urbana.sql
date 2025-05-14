{{
    config(
        alias="pegada_urbana",
        schema="br_geobr_mapas",
        materialized="table",
    )
}}
select
    safe_cast(replace(id_pegada_urbana, ".0", "") as string) id_pegada_urbana,
    safe_cast(replace(id_municipio, ".0", "") as string) id_municipio,
    safe_cast(densidade as string) densidade,
    safe_cast(tipo as string) tipo,
    safe_cast(area as float64) area,
    safe.st_geogfromtext(geometria) geometria,
from {{ set_datalake_project("br_geobr_mapas_staging.pegada_urbana") }} as t
