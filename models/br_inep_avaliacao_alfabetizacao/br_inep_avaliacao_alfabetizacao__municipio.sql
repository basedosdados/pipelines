{{
    config(
        alias="municipio",
        schema="br_inep_avaliacao_alfabetizacao",
        materialized="table",
    )
}}
select
    safe_cast(ano as int64) ano,
    safe_cast(id_municipio as int64) id_municipio,
    safe_cast(serie as string) serie,
    safe_cast(rede as string) rede,
    safe_cast(taxa_alfabetizacao as float64) taxa_alfabetizacao,
    safe_cast(media_portugues as float64) media_portugues,
from
    {{ set_datalake_project("br_inep_avaliacao_alfabetizacao_staging.municipio") }} as t
