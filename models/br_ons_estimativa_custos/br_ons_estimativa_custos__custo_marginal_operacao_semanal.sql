{{
    config(
        alias="custo_marginal_operacao_semanal", schema="br_ons_estimativa_custos"
    )
}}
select
    safe_cast(data as date) data,
    safe_cast(ano as int64) ano,
    safe_cast(mes as int64) mes,
    safe_cast(id_subsistema as string) id_subsistema,
    safe_cast(subsistema as string) subsistema,
    safe_cast(
        custo_marginal_operacao_semanal as float64
    ) custo_marginal_operacao_semanal,
    safe_cast(
        custo_marginal_operacao_semanal_carga_leve as float64
    ) custo_marginal_operacao_semanal_carga_leve,
    safe_cast(
        custo_marginal_operacao_semanal_carga_media as float64
    ) custo_marginal_operacao_semanal_carga_media,
    safe_cast(
        custo_marginal_operacao_semanal_carga_pesada as float64
    ) custo_marginal_operacao_semanal_carga_pesada
from
    {{
        set_datalake_project(
            "br_ons_estimativa_custos_staging.custo_marginal_operacao_semanal"
        )
    }} t
