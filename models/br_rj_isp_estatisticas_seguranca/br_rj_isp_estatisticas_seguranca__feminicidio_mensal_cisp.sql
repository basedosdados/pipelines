{{
    config(
        alias="feminicidio_mensal_cisp", schema="br_rj_isp_estatisticas_seguranca"
    )
}}

select
    safe_cast(t.ano as int64) ano,
    safe_cast(t.mes as int64) mes,
    safe_cast(t.id_cisp as string) id_cisp,
    safe_cast(t.id_aisp as string) id_aisp,
    safe_cast(t.id_risp as string) id_risp,
    m.id_municipio,
    safe_cast(t.quantidade_morte_feminicidio as int64) quantidade_morte_feminicidio,
    safe_cast(
        quantidade_tentativa_feminicidio as int64
    ) quantidade_tentativa_feminicidio,
    safe_cast(tipo_fase as string) tipo_fase
from
    {{
        set_datalake_project(
            "br_rj_isp_estatisticas_seguranca_staging.feminicidio_mensal_cisp"
        )
    }} as t
left join
    `basedosdados-dev.br_bd_diretorios_brasil.municipio` as m on t.id_municipio = m.nome
