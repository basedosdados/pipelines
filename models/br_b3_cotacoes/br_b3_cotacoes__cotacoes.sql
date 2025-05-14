{{
    config(
        alias="cotacoes",
        schema="br_b3_cotacoes",
        materialized="incremental",
        partition_by={
            "field": "data_referencia",
            "data_type": "date",
            "granularity": "day",
        },
        cluster_by="acao_atualizacao",
    )
}}

with
    b3 as (
        select
            safe_cast(data_referencia as date) data_referencia,
            safe_cast(data_negocio as date) data_negocio,
            safe_cast(hora_fechamento as time) hora_fechamento,
            safe_cast(
                codigo_identificador_negocio as string
            ) codigo_identificador_negocio,
            safe_cast(codigo_instrumento as string) codigo_instrumento,
            safe_cast(
                codigo_participante_comprador as string
            ) codigo_participante_comprador,
            safe_cast(
                codigo_participante_vendedor as string
            ) codigo_participante_vendedor,
            safe_cast(acao_atualizacao as string) acao_atualizacao,
            safe_cast(tipo_sessao_pregao as string) tipo_sessao_pregao,
            safe_cast(quantidade_negociada as int64) quantidade_negociada,
            safe_cast(preco_negocio as float64) preco_negocio
        from {{ set_datalake_project("br_b3_cotacoes_staging.cotacoes") }} as t
    )
select *
from b3

# ----- Select the max(data_referencia) timestamp — the most recent record.
# ----- From {{ this }} — the table for this model as it exists in the warehouse, as
# built in our last run.
# ----- So max(data_referencia) FROM {{ this }} the most recent record processed in
# our last run.
{% if is_incremental() %}
    where data_referencia > (select max(data_referencia) from {{ this }})
{% endif %}
