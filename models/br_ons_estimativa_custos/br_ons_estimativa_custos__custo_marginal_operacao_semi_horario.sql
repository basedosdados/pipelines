{{
    config(
        alias="custo_marginal_operacao_semi_horario",
        schema="br_ons_estimativa_custos",
    )
}}

select
    safe_cast(data as date) data,
    safe_cast(hora as time) hora,
    safe_cast(ano as int64) ano,
    safe_cast(mes as int64) mes,
    safe_cast(id_subsistema as string) id_subsistema,
    safe_cast(subsistema as string) subsistema,
    safe_cast(custo_marginal_operacao as float64) custo_marginal_operacao
from
    {{
        set_datalake_project(
            "br_ons_estimativa_custos_staging.custo_marginal_operacao_semi_horario"
        )
    }} t
