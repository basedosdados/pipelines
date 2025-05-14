{{
    config(
        alias="regiao_metropolitana_2017",
        schema="br_geobr_mapas",
        materialized="table",
    )
}}
select
    safe_cast(
        replace(nome_regiao_metropolitana, ".0", "") as string
    ) nome_regiao_metropolitana,
    safe_cast(tipo as string) tipo,
    safe_cast(subcategoria_metropolitana as string) subcategoria_metropolitana,
    safe_cast(replace(id_municipio, ".0", "") as string) id_municipio,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(legislacao as string) legislacao,
    safe_cast(data_legislacao as date) data_legislacao,
    safe.st_geogfromtext(geometria) geometria,
from {{ set_datalake_project("br_geobr_mapas_staging.regiao_metropolitana_2017") }} as t
