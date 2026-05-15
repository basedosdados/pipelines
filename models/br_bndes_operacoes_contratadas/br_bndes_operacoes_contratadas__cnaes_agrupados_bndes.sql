{{
    config(
        alias="cnaes_agrupados_bndes",
        schema="br_bndes_operacoes_contratadas",
        materialized="table",
    )
}}
select
    safe_cast(setor_cnae_bndes as string) setor_cnae_bndes,
    safe_cast(subsetor_agrupado_cnae_bndes as string) subsetor_agrupado_cnae_bndes,
    safe_cast(setor_bndes as string) setor_bndes,
    safe_cast(subsetor_bndes as string) subsetor_bndes,
    safe_cast(secao_cnae as string) secao_cnae,
    safe_cast(divisao_cnae as string) divisao_cnae,
    safe_cast(classe_cnae as string) classe_cnae,
    safe_cast(subclasse_cnae as string) subclasse_cnae,
    safe_cast(produto as string) produto,
from
    {{
        set_datalake_project(
            "br_bndes_operacoes_contratadas_staging.cnaes_agrupados_bndes"
        )
    }} as t
