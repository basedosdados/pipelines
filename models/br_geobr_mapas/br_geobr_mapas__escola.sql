{{
    config(
        alias="escola",
        schema="br_geobr_mapas",
        materialized="table",
    )
}}

select
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_escola as string) id_escola,
    safe.st_geogfromtext(geometria) geometria
from {{ project_path("br_geobr_mapas_staging.escola") }} as t
