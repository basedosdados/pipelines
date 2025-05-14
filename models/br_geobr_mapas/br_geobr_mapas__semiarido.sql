{{
    config(
        alias="semiarido",
        schema="br_geobr_mapas",
        materialized="table",
    )
}}
select
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(sigla_uf as string) sigla_uf,
    safe.st_geogfromtext(geometria) geometria
from {{ set_datalake_project("br_geobr_mapas_staging.semiarido") }} as t
