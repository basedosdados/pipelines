{{
    config(
        materialized="table",
        schema="br_inep_avaliacao_alfabetizacao",
        alias="brasil",
    )
}}

select
    safe_cast(ano as int64) as ano,
    safe_cast(rede as string) as rede,
    round(safe_cast(taxa_alfabetizacao as float64), 2) as media_taxa_alfabetizacao,
    round(safe_cast(meta_alfabetizacao_2024 as float64), 2) as meta_alfabetizacao_2024,
    round(safe_cast(meta_alfabetizacao_2025 as float64), 2) as meta_alfabetizacao_2025,
    round(safe_cast(meta_alfabetizacao_2026 as float64), 2) as meta_alfabetizacao_2026,
    round(safe_cast(meta_alfabetizacao_2027 as float64), 2) as meta_alfabetizacao_2027,
    round(safe_cast(meta_alfabetizacao_2028 as float64), 2) as meta_alfabetizacao_2028,
    round(safe_cast(meta_alfabetizacao_2029 as float64), 2) as meta_alfabetizacao_2029,
    round(safe_cast(meta_alfabetizacao_2030 as float64), 2) as meta_alfabetizacao_2030,
    round(
        safe_cast(percentual_participacao as float64), 2
    ) as media_percentual_participacao,
from `projeto-basedosdados-fl.br_inep_avaliacao_alfabetizacao.uf`
where sigla_uf = "Brasil"
