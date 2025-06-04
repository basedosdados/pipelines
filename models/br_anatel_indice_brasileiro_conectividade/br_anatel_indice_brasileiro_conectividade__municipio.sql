{{
    config(
        alias="municipio",
        schema="br_anatel_indice_brasileiro_conectividade",
        materialized="table",
    )
}}
select
    safe_cast(ano as int64) ano,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(ibc as float64) ibc,
    safe_cast(cobertura_pop_4g5g as float64) cobertura_pop_4g5g,
    safe_cast(fibra as string) fibra,
    safe_cast(densidade_smp as float64) densidade_smp,
    safe_cast(hhi_smp as int64) hhi_smp,
    safe_cast(densidade_scm as float64) densidade_scm,
    safe_cast(hhi_scm as int64) hhi_scm,
    safe_cast(adensamento_estacoes as float64) adensamento_estacoes,
from
    {{
        set_datalake_project(
            "br_anatel_indice_brasileiro_conectividade_staging.municipio"
        )
    }} as t
