{{
    config(
        alias="restricao_operacao_usinas_eolicas",
        schema="br_ons_avaliacao_operacao",
        materialized="incremental",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2021, "end": 2024, "interval": 1},
        },
    )
}}
with
    ons as (
        select
            safe_cast(data as date) data,
            safe_cast(hora as time) hora,
            safe_cast(ano as int64) ano,
            safe_cast(mes as int64) mes,
            safe_cast(sigla_uf as string) sigla_uf,
            safe_cast(id_subsistema as string) id_subsistema,
            safe_cast(subsistema as string) subsistema,
            safe_cast(id_ons as string) id_ons,
            safe_cast(
                replace(id_empreendimento_aneel, '-', '') as string
            ) id_empreendimento_aneel,
            safe_cast(usina as string) usina,
            safe_cast(
                replace(tipo_razao_restricao, 'nan', '') as string
            ) tipo_razao_restricao,
            safe_cast(
                replace(tipo_origem_restricao, 'nan', '') as string
            ) tipo_origem_restricao,
            safe_cast(geracao as float64) geracao,
            safe_cast(geracao_limitada as float64) geracao_limitada,
            safe_cast(disponibilidade as float64) disponibilidade,
            safe_cast(geracao_referencia as float64) geracao_referencia,
            safe_cast(geracao_referencia_final as float64) geracao_referencia_final
        from
            {{
                set_datalake_project(
                    "br_ons_avaliacao_operacao_staging.restricao_operacao_usinas_eolicas"
                )
            }}
            as t
    )
select distinct *
from ons
{% if is_incremental() %} where data > (select max(data) from {{ this }}) {% endif %}
