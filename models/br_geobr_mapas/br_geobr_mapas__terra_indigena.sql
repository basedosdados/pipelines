{{
    config(
        alias="terra_indigena",
        schema="br_geobr_mapas",
        materialized="table",
    )
}}
select
    safe_cast(id_geografico as string) id_geografico,
    safe_cast(id_terra_indigena as string) id_terra_indigena,
    safe_cast(terra_indigena as string) terra_indigena,
    safe_cast(etnia as string) etnia,
    safe_cast(nome_municipio as string) nome_municipio,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(area as float64) area,
    safe_cast(fase as string) fase,
    safe_cast(modalidade as string) modalidade,
    safe_cast(reestudo as string) reestudo,
    safe_cast(indicador_faixa_fronteira as bool) indicador_faixa_fronteira,
    safe_cast(
        replace(id_unidade_administrativa, ".0", "") as string
    ) id_unidade_administrativa,
    safe_cast(sigla_unidade_administrativa as string) sigla_unidade_administrativa,
    initcap(unidade_administrativa) unidade_administrativa,
    safe.st_geogfromtext(geometria) geometria,
from {{ set_datalake_project("br_geobr_mapas_staging.terra_indigena") }} as t
