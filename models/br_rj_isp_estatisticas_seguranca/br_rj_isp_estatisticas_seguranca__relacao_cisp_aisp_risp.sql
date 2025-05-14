{{ config(alias="relacao_cisp_aisp_risp", schema="br_rj_isp_estatisticas_seguranca") }}

select
    safe_cast(id_risp as string) id_risp,
    safe_cast(id_aisp as string) id_aisp,
    safe_cast(id_cisp as string) id_cisp,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(unidade_territorial as string) unidade_territorial,
    safe_cast(regiao as string) regiao
from
    {{
        set_datalake_project(
            "br_rj_isp_estatisticas_seguranca_staging.relacao_cisp_aisp_risp"
        )
    }} t
