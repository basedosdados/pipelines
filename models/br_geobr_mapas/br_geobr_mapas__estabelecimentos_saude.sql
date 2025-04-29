{{
    config(
        alias="estabelecimentos_saude",
        schema="br_geobr_mapas",
        materialized="table",
    )
}}
select
    safe_cast(ano as int64) ano,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(id_cnes as string) id_cnes,
    safe.st_geogfromtext(geometria) geometria
from {{ set_datalake_project("br_geobr_mapas_staging.estabelecimentos_saude") }} as t
