{{
    config(
        alias="feminicidio_mensal_cisp", schema="br_rj_isp_estatisticas_seguranca"
    )
}}

select
    safe_cast(ano as int64) ano,
    safe_cast(mes as int64) mes,
    safe_cast(id_cisp as string) id_cisp,
    safe_cast(quantidade_morte_feminicidio as int64) quantidade_morte_feminicidio,
    safe_cast(
        quantidade_tentativa_feminicidio as int64
    ) quantidade_tentativa_feminicidio,
    safe_cast(tipo_fase as string) tipo_fase
from
    {{
        set_datalake_project(
            "br_rj_isp_estatisticas_seguranca_staging.feminicidio_mensal_cisp"
        )
    }} t
