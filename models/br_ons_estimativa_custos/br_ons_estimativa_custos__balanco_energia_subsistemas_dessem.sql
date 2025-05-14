{{
    config(
        alias="balanco_energia_subsistemas_dessem",
        schema="br_ons_estimativa_custos",
        cluster_by=["ano", "mes"],
    )
}}

select
    safe_cast(data as date) data,
    safe_cast(hora as time) hora,
    safe_cast(ano as int64) ano,
    safe_cast(mes as int64) mes,
    safe_cast(id_subsistema as string) id_subsistema,
    safe_cast(subsistema as string) subsistema,
    safe_cast(valor_demanda as float64) valor_demanda,
    safe_cast(usina_hidraulica_verificada as float64) usina_hidraulica_verificada,
    safe_cast(
        geracao_pequena_usina_hidraulica_verificada as float64
    ) geracao_pequena_usina_hidraulica_verificada,
    safe_cast(
        geracao_usina_termica_verificada as float64
    ) geracao_usina_termica_verificada,
    safe_cast(
        geracao_pequena_usina_termica_verificada as float64
    ) geracao_pequena_usina_termica_verificada,
    safe_cast(geracao_eolica_verificada as float64) geracao_eolica_verificada,
    safe_cast(
        geracao_fotovoltaica_verificada as float64
    ) geracao_fotovoltaica_verificada
from
    {{
        set_datalake_project(
            "br_ons_estimativa_custos_staging.balanco_energia_subsistemas_dessem"
        )
    }} t
