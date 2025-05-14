{{
    config(
        alias="geracao_termica_motivo_despacho",
        schema="br_ons_avaliacao_operacao",
        materialized="incremental",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2013, "end": 2024, "interval": 1},
        },
        cluster_by=["ano", "mes"],
    )
}}

with
    ons as (
        select
            safe_cast(data as date) data,
            safe_cast(hora as time) hora,
            safe_cast(ano as int64) ano,
            safe_cast(mes as int64) mes,
            safe_cast(id_subsistema as string) id_subsistema,
            safe_cast(subsistema as string) subsistema,
            safe_cast(
                replace(id_usina_planejamento, '.0', '') as string
            ) id_usina_planejamento,
            safe_cast(usina as string) usina,
            safe_cast(tipo_patamar as string) tipo_patamar,
            safe_cast(atendimento_satisfatorio as int64) atendimento_satisfatorio,
            safe_cast(geracao_programada_total as float64) geracao_programada_total,
            safe_cast(
                geracao_programada_ordem_merito as float64
            ) geracao_programada_ordem_merito,
            safe_cast(
                geracao_programada_referencia_ordem_merito as float64
            ) geracao_programada_referencia_ordem_merito,
            safe_cast(
                geracao_programada_inflexibilidade as float64
            ) geracao_programada_inflexibilidade,
            safe_cast(
                geracao_programada_razao_eletrica as float64
            ) geracao_programada_razao_eletrica,
            safe_cast(
                geracao_programada_seguranca_energetica as float64
            ) geracao_programada_seguranca_energetica,
            safe_cast(
                geracao_programada_sem_ordem_merito as float64
            ) geracao_programada_sem_ordem_merito,
            safe_cast(
                geracao_programada_reposicao_perdas as float64
            ) geracao_programada_reposicao_perdas,
            safe_cast(
                geracao_programada_exportacao as float64
            ) geracao_programada_exportacao,
            safe_cast(
                geracao_programada_reserva_potencia as float64
            ) geracao_programada_reserva_potencia,
            safe_cast(
                geracao_programada_substituicao as float64
            ) geracao_programada_substituicao,
            safe_cast(
                geracao_programada_unit_commitment as float64
            ) geracao_programada_unit_commitment,
            safe_cast(
                geracao_programada_constrained_off as float64
            ) geracao_programada_constrained_off,
            safe_cast(geracao_verificada as float64) geracao_verificada,
            safe_cast(ordem_merito_verificada as float64) ordem_merito_verificada,
            safe_cast(
                geracao_inflexibilidade_verificada as float64
            ) geracao_inflexibilidade_verificada,
            safe_cast(
                geracao_razao_eletrica_verificada as float64
            ) geracao_razao_eletrica_verificada,
            safe_cast(
                geracao_seguranca_energetica_verificada as float64
            ) geracao_seguranca_energetica_verificada,
            safe_cast(
                geracao_sem_ordem_merito_verificada as float64
            ) geracao_sem_ordem_merito_verificada,
            safe_cast(
                geracao_reposicao_perdas_verificada as float64
            ) geracao_reposicao_perdas_verificada,
            safe_cast(
                geracao_exportacao_verificada as float64
            ) geracao_exportacao_verificada,
            safe_cast(
                geracao_reserva_potencia_verificada as float64
            ) geracao_reserva_potencia_verificada,
            safe_cast(
                geracao_substituicao_verificada as float64
            ) geracao_substituicao_verificada,
            safe_cast(
                geracao_unit_commitment_verificada as float64
            ) geracao_unit_commitment_verificada,
            safe_cast(
                geracao_constrained_off_verificada as float64
            ) geracao_constrained_off_verificada
        from
            {{
                set_datalake_project(
                    "br_ons_avaliacao_operacao_staging.geracao_termica_motivo_despacho"
                )
            }} as t
    )
select *
from ons
{% if is_incremental() %} where data > (select max(data) from {{ this }}) {% endif %}
