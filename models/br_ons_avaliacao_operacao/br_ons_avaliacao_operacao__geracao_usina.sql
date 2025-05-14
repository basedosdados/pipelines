{{
    config(
        alias="geracao_usina",
        schema="br_ons_avaliacao_operacao",
        materialized="incremental",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2000, "end": 2024, "interval": 1},
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
            safe_cast(
                replace(id_empreendimento_aneel, '-', '') as string
            ) id_empreendimento_aneel,
            safe_cast(usina as string) usina,
            safe_cast(tipo_usina as string) tipo_usina,
            safe_cast(tipo_modalidade_operacao as string) tipo_modalidade_operacao,
            safe_cast(tipo_combustivel as string) tipo_combustivel,
            safe_cast(geracao as float64) geracao
        from
            {{
                set_datalake_project(
                    "br_ons_avaliacao_operacao_staging.geracao_usina"
                )
            }} as t
    )
select distinct *
from ons
{% if is_incremental() %} where data > (select max(data) from {{ this }}) {% endif %}
