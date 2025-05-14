{{ config(alias="balanco_energia_subsistemas", schema="br_ons_estimativa_custos") }}

select
    safe_cast(data as date) data,
    safe_cast(hora as time) hora,
    safe_cast(ano as int64) ano,
    safe_cast(mes as int64) mes,
    safe_cast(id_subsistema as string) id_subsistema,
    safe_cast(subsistema as string) subsistema,
    safe_cast(geracao_hidraulica_verificada as float64) geracao_hidraulica_verificada,
    safe_cast(geracao_termica_verificada as float64) geracao_termica_verificada,
    safe_cast(geracao_eolica_verificada as float64) geracao_eolica_verificada,
    safe_cast(
        geracao_fotovoltaica_verificada as float64
    ) geracao_fotovoltaica_verificada,
    safe_cast(carga_verificada as float64) carga_verificada,
    safe_cast(intercambio_verificado as float64) intercambio_verificado
from
    {{
        set_datalake_project(
            "br_ons_estimativa_custos_staging.balanco_energia_subsistemas"
        )
    }} t
