{{
    config(
        alias="taxa_escolarizacao_tea_raca_cor_grupo_idade",
        schema="br_ibge_censo_2022",
        materialized="table",
    )
}}
select
    safe_cast(ano as int64) ano,
    safe_cast(diagnostico as string) diagnostico,
    safe_cast(grupo_idade as string) grupo_idade,
    safe_cast(raca_cor as string) raca_cor,
    safe_cast(taxa_escolarizacao as float64) taxa_escolarizacao,
from
    {{
        set_datalake_project(
            "br_ibge_censo_2022_staging.taxa_escolarizacao_tea_raca_cor_grupo_idade"
        )
    }} as t
