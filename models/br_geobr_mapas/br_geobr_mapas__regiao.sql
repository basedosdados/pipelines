{{
    config(
        alias="regiao",
        schema="br_geobr_mapas",
        materialized="table",
    )
}}
select
    safe_cast(id_regiao as string) id_regiao,
    safe_cast(nome_regiao as string) nome_regiao,
    safe.st_geogfromtext(geometria) geometria
from {{ set_datalake_project("br_geobr_mapas_staging.regiao") }} as t
