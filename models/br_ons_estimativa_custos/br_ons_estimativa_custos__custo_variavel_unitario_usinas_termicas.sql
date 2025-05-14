{{
    config(
        alias="custo_variavel_unitario_usinas_termicas",
        schema="br_ons_estimativa_custos",
        materialized="incremental",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {
                "start": 2019,
                "end": 2024,
                "interval": 1,
            },
        },
        cluster_by=["ano", "mes"],
    )
}}
with
    ons as (
        select
            safe_cast(data_inicio as date) data_inicio,
            safe_cast(ano as int64) ano,
            safe_cast(mes as int64) mes,
            safe_cast(data_fim as date) data_fim,
            safe_cast(ano as int64) ano_pmo,
            safe_cast(mes as int64) mes_pmo,
            safe_cast(numero_revisao as int64) numero_revisao,
            safe_cast(semana_operativa as string) semana_operativa,
            safe_cast(id_modelo_usina as string) id_modelo_usina,
            safe_cast(id_subsistema as string) id_subsistema,
            safe_cast(subsistema as string) subsistema,
            safe_cast(usina as string) usina,
            safe_cast(custo_variavel_unitario as float64) custo_variavel_unitario
        from
            {{
                set_datalake_project(
                    "br_ons_estimativa_custos_staging.custo_variavel_unitario_usinas_termicas"
                )
            }}
            as t
    )
select *
from ons
{% if is_incremental() %}
    where data_inicio > (select max(data_inicio) from {{ this }})
{% endif %}
