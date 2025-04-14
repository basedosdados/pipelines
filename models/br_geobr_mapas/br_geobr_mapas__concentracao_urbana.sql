{{
    config(
        alias="concentracao_urbana",
        schema="br_geobr_mapas",
        materialized="table",
    )
}}
select
    safe_cast(
        replace(id_concentracao_urbana, ".0", "") as string
    ) id_concentracao_urbana,
    safe_cast(concentracao_urbana as string) concentracao_urbana,
    safe_cast(populacao_urbana_2010 as int64) populacao_urbana_2010,
    safe_cast(populacao_rural_2010 as int64) populacao_rural_2010,
    safe_cast(replace(populacao_2010, ".0", "") as int64) populacao_2010,
    safe_cast(replace(id_municipio, ".0", "") as string) id_municipio,
    safe_cast(sigla_uf as string) sigla_uf,
    safe.st_geogfromtext(geometria) geometria,
from {{ set_datalake_project("br_geobr_mapas_staging.concentracao_urbana") }} as t
