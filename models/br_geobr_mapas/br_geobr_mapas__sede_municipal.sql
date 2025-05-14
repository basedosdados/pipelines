{{
    config(
        alias="sede_municipal",
        schema="br_geobr_mapas",
        materialized="table",
    )
}}
select
    safe_cast(ano as int64) ano,
    safe_cast(replace(id_municipio, ".0", "") as string) id_municipio,
    initcap(nome_municipio) nome_municipio,
    safe_cast(replace(id_uf, ".0", "") as string) id_uf,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_regiao as string) id_regiao,
    safe_cast(regiao as string) regiao,
    safe.st_geogfromtext(geometria) geometria,
from {{ set_datalake_project("br_geobr_mapas_staging.sede_municipal") }} as t
