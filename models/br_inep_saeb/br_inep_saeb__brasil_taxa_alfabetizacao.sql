{{
    config(
        alias="brasil_taxa_alfabetizacao",
        schema="br_inep_saeb",
        materialized="table",
    )
}}
select
    safe_cast(ano as int64) ano,
    safe_cast(rede as string) rede,
    safe_cast(localizacao as string) localizacao,
    safe_cast(area as string) area,
    safe_cast(taxa_alfabetizacao as float64) taxa_alfabetizacao,
from {{ project_path("br_inep_saeb_staging.brasil_taxa_alfabetizacao") }} as t
