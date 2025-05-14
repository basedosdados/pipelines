{{
    config(
        alias="unidade_conservacao",
        schema="br_geobr_mapas",
        materialized="table",
    )
}}
select
    safe_cast(id_unidade_conservacao as string) id_unidade_conservacao,
    initcap(unidade_conservacao) unidade_conservacao,
    safe_cast(id_unidade_conservacao_wcmc as string) id_unidade_conservacao_wcmc,
    safe_cast(id_cnuc as string) id_cnuc,
    safe_cast(id_geografico as string) id_geografico,
    safe_cast(organizacao_orgao as string) organizacao_orgao,
    safe_cast(categoria as string) categoria,
    safe_cast(sigla_grupo as string) sigla_grupo,
    safe_cast(qualidade as string) qualidade,
    initcap(esfera) esfera,
    safe_cast(ano_criacao as int64) ano_criacao,
    safe_cast(legislacao as string) legislacao,
    safe_cast(data_ultima as date) data_ultima,
    safe.st_geogfromtext(geometria) geometria,
from {{ set_datalake_project("br_geobr_mapas_staging.unidade_conservacao") }} as t
