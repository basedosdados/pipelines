{{
    config(
        alias="armas_apreendidas_mensal", schema="br_rj_isp_estatisticas_seguranca"
    )
}}

select
    safe_cast(ano as int64) ano,
    safe_cast(mes as int64) mes,
    safe_cast(id_cisp as string) id_cisp,
    safe_cast(id_aisp as string) id_aisp,
    safe_cast(id_risp as string) id_risp,
    safe_cast(
        quantidade_arma_fabricacao_caseira as int64
    ) quantidade_arma_fabricacao_caseira,
    safe_cast(quantidade_carabina as int64) quantidade_carabina,
    safe_cast(quantidade_espingarda as int64) quantidade_espingarda,
    safe_cast(quantidade_fuzil as int64) quantidade_fuzil,
    safe_cast(quantidade_garrucha as int64) quantidade_garrucha,
    safe_cast(quantidade_garruchao as int64) quantidade_garruchao,
    safe_cast(quantidade_metralhadora as int64) quantidade_metralhadora,
    safe_cast(quantidade_outros as int64) quantidade_outros,
    safe_cast(quantidade_pistola as int64) quantidade_pistola,
    safe_cast(quantidade_revolver as int64) quantidade_revolver,
    safe_cast(quantidade_submetralhadora as int64) quantidade_submetralhadora,
    safe_cast(total as int64) total
from
    {{
        set_datalake_project(
            "br_rj_isp_estatisticas_seguranca_staging.armas_apreendidas_mensal"
        )
    }} t
