{{
    config(
        alias="limite_vizinhanca",
        schema="br_geobr_mapas",
        materialized="table",
    )
}}
select
    safe_cast(replace(id_uf, ".0", "") as string) id_uf,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(replace(id_municipio, ".0", "") as string) id_municipio,
    safe_cast(nome_municipio as string) nome_municipio,
    safe_cast(replace(id_distrito, ".0", "") as string) id_distrito,
    safe_cast(nome_distrito as string) nome_distrito,
    safe_cast(replace(id_subdistrito, ".0", "") as string) id_subdistrito,
    safe_cast(nome_subdistrito as string) nome_subdistrito,
    safe_cast(replace(id_vizinhanca, ".0", "") as string) id_vizinhanca,
    safe_cast(nome_vizinhanca as string) nome_vizinhanca,
    safe_cast(referencia_geometria as string) referencia_geometria,
    safe.st_geogfromtext(geometria) geometria,
from {{ set_datalake_project("br_geobr_mapas_staging.limite_vizinhanca") }} as t
