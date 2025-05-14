{{
    config(
        alias="area_risco_desastre",
        schema="br_geobr_mapas",
        materialized="table",
    )
}}
select
    safe_cast(replace(geocodigo_bater, ".0", "") as string) geocodigo_bater,
    safe_cast(origem as string) origem,
    safe_cast(acuracia as string) acuracia,
    safe_cast(observacao as string) observacao,
    safe_cast(quantidade_poligono as int64) quantidade_poligono,
    safe_cast(replace(id_municipio, ".0", "") as string) id_municipio,
    safe_cast(sigla_uf as string) sigla_uf,
    safe.st_geogfromtext(geometria) geometria,
from {{ set_datalake_project("br_geobr_mapas_staging.area_risco_desastre") }} as t
