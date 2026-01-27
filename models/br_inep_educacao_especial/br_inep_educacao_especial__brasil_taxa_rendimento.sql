{{
    config(
        alias="brasil_taxa_rendimento",
        schema="br_inep_educacao_especial",
        materialized="table",
    )
}}

select
    safe_cast(ano as int64) ano,
    safe_cast(etapa_ensino as string) etapa_ensino,
    safe_cast(taxa_aprovacao as float64) taxa_aprovacao,
    safe_cast(taxa_reprovacao as float64) taxa_reprovacao,
    safe_cast(taxa_abandono as float64) taxa_abandono,
from
    {{
        set_datalake_project(
            "br_inep_educacao_especial_staging.brasil_taxa_rendimento"
        )
    }} as t
