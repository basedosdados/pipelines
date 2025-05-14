{{
    config(
        alias="armas_fogo_apreendidas_mensal",
        schema="br_rj_isp_estatisticas_seguranca",
    )
}}

select
    safe_cast(ano as int64) ano,
    safe_cast(mes as int64) mes,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(id_cisp as string) id_cisp,
    safe_cast(id_aisp as string) id_aisp,
    safe_cast(id_risp as string) id_risp,
    safe_cast(quantidade_arma_fogo_apreendida as int64) quantidade_arma_fogo_apreendida
from
    {{
        set_datalake_project(
            "br_rj_isp_estatisticas_seguranca_staging.armas_fogo_apreendidas_mensal"
        )
    }} as t
