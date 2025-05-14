{{
    config(
        alias="municipio",
        schema="br_geobr_mapas",
        materialized="table",
    )
}}

select
    safe_cast(replace(code_muni, '.0', '') as string) id_municipio,
    safe_cast(abbrev_state as string) sigla_uf,
    safe.st_geogfromtext(geometry) geometria
from {{ set_datalake_project("br_geobr_mapas_staging.municipio") }} as t
