{{
    config(
        alias="meta_alfabetizacao_brasil",
        schema="br_inep_avaliacao_alfabetizacao",
        materialized="table",
    )
}}
select
    safe_cast(ano as int64) ano,
    safe_cast(rede as string) rede,
    safe_cast(taxa_alfabetizacao as float64) taxa_alfabetizacao,
    safe_cast(meta_alfabetizacao_2024 as float64) meta_alfabetizacao_2024,
    safe_cast(meta_alfabetizacao_2025 as float64) meta_alfabetizacao_2025,
    safe_cast(meta_alfabetizacao_2026 as float64) meta_alfabetizacao_2026,
    safe_cast(meta_alfabetizacao_2027 as float64) meta_alfabetizacao_2027,
    safe_cast(meta_alfabetizacao_2028 as float64) meta_alfabetizacao_2028,
    safe_cast(meta_alfabetizacao_2029 as float64) meta_alfabetizacao_2029,
    safe_cast(meta_alfabetizacao_2030 as float64) meta_alfabetizacao_2030,
    safe_cast(percentual_participacao as float64) percentual_participacao,
from
    {{
        set_datalake_project(
            "br_inep_avaliacao_alfabetizacao_staging.meta_alfabetizacao_brasil"
        )
    }} as t
